#include "g3log/g3log.hpp"
#include <czmq.h>
#include <zctx.h>
#include <zsocket.h>
#include <zmsg.h>
#include <zsocket.h>
#include <zsockopt.h>
#include <zframe.h>
#include <iostream>
#include <time.h>
#include <vector>
#include "boost/uuid/uuid_io.hpp"
#include <thread>
#include <chrono>
#include "QueueNadoMacros.h"
#include "BoomStick.h"
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/random_device.hpp>
namespace {

   void ShrinkToFit(std::map<std::string, std::string>& map) {
      std::map<std::string, std::string>(map).swap(map);
   }

   void ShrinkToFit(std::map<std::string, time_t>& map) {
      std::map<std::string, time_t>(map).swap(map);
   }

}

/**
 * Construct with a ZMQ socket binding
 * @param binding
 *   The binding is stored, but Initialize must be used to connect to it.
 */
BoomStick::BoomStick(const std::string& binding) : mLastGCTime(time(NULL)),
mBinding(binding), mChamber(nullptr), mCtx(nullptr), mRan(), m_uuidGen(mRan),
mSendHWM(1000), mRecvHWM(1000), mPendingAlertSize(500), mUnreadAlertSize(500),
mUnreadAlert(false), mPendingAlert(false), mUtilizedThread(0) {
//mRan.seed(boost::uuids::detail::seed_rng()());
boost::random::mt19937 gen(rng());

}

/**
 * Deconstruct
 *   This destroys the context and any associated sockets
 */
BoomStick::~BoomStick() {
   if (mCtx != nullptr) {
      zctx_destroy(&mCtx);
   }
   if (!mPendingReplies.empty()) {
      LOG(WARNING) << "Pending replies never emptied " << mPendingReplies.size();
      for (auto reply : mPendingReplies) {
         LOG(WARNING) << reply.first;
      }
   }
   if (!mUnreadReplies.empty()) {
      LOG(WARNING) << "mUnreadReplies replies never emptied " << mUnreadReplies.size();
      for (auto reply : mUnreadReplies) {
         LOG(WARNING) << reply.first;
      }
   }
}

/**
 * Get the context (can be safely shared between threads)
 * @return 
 */
zctx_t* BoomStick::GetContext() {
   return mCtx;
}

/**
 * Swap internals
 * @param other
 */
void BoomStick::Swap(BoomStick& other) {
   //LOG(DEBUG) << "BoomStickSwapping";
   mBinding = other.mBinding;
   mChamber = other.mChamber;
   mCtx = other.mCtx;
   mLastGCTime = other.mLastGCTime;
   mRan = other.mRan;
   m_uuidGen = other.m_uuidGen;
   mSendHWM = other.mSendHWM;
   mRecvHWM = other.mRecvHWM;
   mPendingAlertSize = other.mPendingAlertSize;
   mUnreadAlertSize = other.mUnreadAlertSize;
   mUnreadAlert = other.mUnreadAlert;
   mPendingAlert = other.mPendingAlert;
   mUtilizedThread = other.mUtilizedThread;
   
   //   other.mBinding.clear();  Allow it to be initialized again
   other.mPendingAlertSize = 0;
   other.mUnreadAlertSize = 0;
   other.mUnreadAlert = false;
   other.mPendingAlert = false;
   other.mChamber = nullptr;
   other.mCtx = nullptr;
   other.mUtilizedThread = 0;
}

/**
 * Set the High water for sending messages, only works before the socket connects
 * @param hwm
 */
void BoomStick::SetSendHWM(const int hwm) {
   mSendHWM = hwm;
}

/**
 * Set the High water for receiving messages, only works before the socket connects
 * @param hwm
 */
void BoomStick::SetRecvHWM(const int hwm) {
   mRecvHWM = hwm;
}

/**
 * Move constructor
 * @param other
 *   A BoomStick that is presumably setup already
 */
BoomStick::BoomStick(BoomStick&& other) {
   Swap(other);
}

/**
 * Move assignment operator
 * @param other
 *   A Boomstick that is presumably setup already
 * @return 
 *   A reference to this
 */
BoomStick& BoomStick::operator=(BoomStick&& other) {
   if (this != &other) {
      if (nullptr != mCtx) {
         zctx_destroy(&mCtx);
      }
      Swap(other);
   }
   return *this;
}

/**
 * Get a brand new ZMQ context
 * @return 
 *   A pointer to the context
 */
zctx_t* BoomStick::GetNewContext() {
   zctx_t* context = zctx_new();
   return context;
}

/**
 * Open a new socket (DEALER) on the given context
 * @param ctx
 *   An existing context
 * @return 
 *   A pointer to a socket
 */
void* BoomStick::GetNewSocket(zctx_t* ctx) {
   if (nullptr == ctx) {
      return nullptr;
   }
   return zsocket_new(ctx, ZMQ_DEALER);
}

std::string BoomStick::GetUuid() {
   boost::uuids::uuid u1 = m_uuidGen();

   std::stringstream ss;
   ss << u1;

   return ss.str();
}

/**
 * Connect the given socket to the given binding
 * @param socket
 *   An existing socket
 * @param binding
 *   A string binding 
 * @return 
 *   If connection was successful
 */
bool BoomStick::ConnectToBinding(void* socket, const std::string& binding) {
   if (nullptr == socket) {
      return false;
   }
   zsocket_set_sndhwm(socket, mSendHWM);
   zsocket_set_rcvhwm(socket, mRecvHWM);
   
   return (zsocket_connect(socket, binding.c_str()) >= 0);
}

/**
 * set the binding to something else, de-initialize
 * @param binding
 */
void BoomStick::SetBinding(const std::string& binding) {
   if (nullptr != mCtx) {
      zctx_destroy(&mCtx);
      mCtx = nullptr;
      mChamber = nullptr;
   }
   mBinding = binding;
}

/**
 * Initialize the context, socket and connect to the bound address
 * @return 
 *   true when successful
 */
bool BoomStick::Initialize() {
   if (nullptr != mCtx) {
      return true;
   }
   mCtx = GetNewContext();
   if (nullptr == mCtx) {
      LOG(WARNING) << "queue error " << zmq_strerror(zmq_errno());
      return false;
   }
   mChamber = GetNewSocket(mCtx);
   // The memory in this pointer is managed by the context and should not be deleted
   if (nullptr == mChamber) {
      LOG(WARNING) << "queue error " << zmq_strerror(zmq_errno());
      zctx_destroy(&mCtx);
      mCtx = nullptr;
      return false;
   }
   if (!ConnectToBinding(mChamber, mBinding)) {

      zctx_destroy(&mCtx);
      mChamber = nullptr;
      mCtx = nullptr;
      return false;
   }
   return true;
}

/**
 * Is the given uuid in the list of pending requests
 * @param uuid
 * @return 
 */
bool BoomStick::FindPendingUuid(const std::string& uuid) const {
   return mPendingReplies.find(uuid) != mPendingReplies.end();
}

/**
 * Is the given uuid in the list of unread replies
 * @param uuid
 * @return 
 */
bool BoomStick::FindUnreadUuid(const std::string& uuid) const {
   return mUnreadReplies.find(uuid) != mUnreadReplies.end();
}

/**
 * A Synchronous send with a blocking receive.
 * 
 * @param command
 *   A string to send
 * @return 
 *   The reply received
 */
std::string BoomStick::Send(const std::string& command) {
   const std::string uuid = GetUuid();
   if (mUtilizedThread == 0) {
      mUtilizedThread = pthread_self();
   } else {
      CHECK(mUtilizedThread == pthread_self());
   }
   if (!SendAsync(uuid, command)) {
      return
      {
      };
   }
   std::string returnString;
   if (!GetAsyncReply(uuid, 30000, returnString)) {
      return
      {
      };
   }
   return returnString;
}

/**
 * Send a message, but leave the reply on the socket
 * @param uuid
 *   A unique identifier for this send
 * @param command
 *   The string that will be sent
 * @return 
 *   If the send was successful
 */
bool BoomStick::SendAsync(const std::string& uuid, const std::string& command) {
   if (0 == mUtilizedThread) {
      mUtilizedThread = pthread_self();
   } else {
      CHECK(pthread_self() == mUtilizedThread);
   }
   if (nullptr == mCtx || nullptr == mChamber) {
      return false;
   }
   bool success = true;
   if (FindPendingUuid(uuid)) {
      return true;
   }
   zmsg_t* msg = zmsg_new();
   if (zmsg_addmem(msg, uuid.c_str(), uuid.size()) < 0) {
      success = false;
      LOG(WARNING) << "queue error " << zmq_strerror(zmq_errno());
   } else if (zmsg_addmem(msg, command.c_str(), command.size()) < 0) {
      success = false;
      LOG(WARNING) << "queue error " << zmq_strerror(zmq_errno());
   } else {
      zmq_pollitem_t items[1];
      items[0].socket = mChamber;
      items[0].events = ZMQ_POLLOUT;
      int rc = zmq_poll(items, 1, 0);
      if (0 == rc) {
         zmq_poll(items,1,100);
      }
      if (rc < 0) {
         success = false;
         LOG(WARNING) << "Queue error, cannot poll for status";

      } else if (1 == rc) {
         if ((items[0].revents & ZMQ_POLLOUT) != ZMQ_POLLOUT) {
            LOG(WARNING) << "Queue error, cannot send messages the queue is full";
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            success = false;
         } else if (zmsg_send(&msg, mChamber) == 0) {
            success = true;
            mPendingReplies[uuid] = std::time(NULL);
         } else {
            LOG(WARNING) << "queue error " << zmq_strerror(zmq_errno());
            success = false;
         }
      } else {
         LOG(WARNING) << "Queue error, timeout waiting for queue to be ready";
         success = false;
      }
   }
   if (msg) {

      zmsg_destroy(&msg);
   }

   return success;
}

/**
 * Attempt to grab the reply from the previously read messages
 * 
 * @param messageHash
 * @param reply
 * @return 
 */
bool BoomStick::GetReplyFromCache(const std::string& messageHash, std::string& reply) {
   if (mUnreadReplies.find(messageHash) != mUnreadReplies.end()) {
      reply = mUnreadReplies[messageHash];
      mUnreadReplies.erase(messageHash);
      if (mPendingReplies.find(messageHash) != mPendingReplies.end()) {
         mPendingReplies.erase(messageHash);
      } else {

         LOG(WARNING) << "Found reply in cache, but it was never pending" << messageHash;
      }
      return true;
   }
   return false;
}

/**
 * Poll the socket, fail after timeout and log
 * @param messageHash
 * @return 
 */
bool BoomStick::CheckForMessagePending(const std::string& messageHash, const unsigned int msToWait, std::string& reply) {
   if (0 == mUtilizedThread) {
      mUtilizedThread = pthread_self();
   } else {
      CHECK(pthread_self() == mUtilizedThread);
   }
   if (!mChamber) {
      LOG(WARNING) << "Invalid socket";
      return false;
   }
   if (!zsocket_poll(mChamber, msToWait)) {
      reply = "socket timed out";
      return false;
   }
   return true;
}

/**
 * 
 * @param foundId
 * @param foundReply
 * @return 
 */
bool BoomStick::ReadFromReadySocket(std::string& foundId, std::string& foundReply) {
   if (0 == mUtilizedThread) {
      mUtilizedThread = pthread_self();
   } else {
      CHECK(pthread_self() == mUtilizedThread);
   }
   if (!mChamber) {
      LOG(WARNING) << "Invalid socket";
      return false;
   }
   bool success = false;
   zmsg_t* msg = zmsg_recv(mChamber);
   if (!msg) {
      foundReply = zmq_strerror(zmq_errno());
   } else if (zmsg_size(msg) == 2) {
      char* msgChar;
      msgChar = zmsg_popstr(msg);
      foundId = msgChar;
      free(msgChar);
      msgChar = zmsg_popstr(msg);
      foundReply = msgChar;
      free(msgChar);
      success = true;
   } else {
      foundReply = "Malformed reply, expecting 2 parts";
   }

   if (msg) {

      zmsg_destroy(&msg);
   }
   return success;
}

/**
 * Pull from the socket till the requested reply is found
 * 
 * @param uuid
 *   An id of a message that has previously been sent
 * @param reply
 *   The reply 
 * @return 
 *   if the pull was successful.  Can timeout and return false.  Error info will 
 * be in the reply return. 
 */
bool BoomStick::GetAsyncReply(const std::string& uuid, const unsigned int msToWait, std::string& reply) {
   if (0 == mUtilizedThread) {
      mUtilizedThread = pthread_self();
   } else {
      CHECK(pthread_self() == mUtilizedThread);
   }
   if (nullptr == mCtx || nullptr == mChamber) {
      LOG(WARNING) << "Invalid socket";
      reply = "No socket";
      CleanOldPendingData();
      return false;
   }

   bool found = GetReplyFromCache(uuid, reply);
   if (!found) {
      found = GetReplyFromSocket(uuid, msToWait, reply);
   }
   CleanOldPendingData();

   return found;
}

/**
 * Check the socket for a specific reply, also fill the cache when other replies 
 *   are seen
 * @param messageHash
 * @param reply
 *   Either the reply, or when an error occurs an error message
 * @return 
 *   If the message was found
 */
bool BoomStick::GetReplyFromSocket(const std::string& uuid, const unsigned int msToWait, std::string& reply) {
   if (0 == mUtilizedThread) {
      mUtilizedThread = pthread_self();
   } else {
      CHECK(pthread_self() == mUtilizedThread );
   }
   bool found = false;
   reply = "Timed out searching for reply";
   while (!zctx_interrupted && !found && CheckForMessagePending(uuid, msToWait, reply)) {
      std::string foundId;
      if (!ReadFromReadySocket(foundId, reply)) {
         break;
      }
      if (uuid == foundId) {
         found = true;
         mPendingReplies.erase(uuid);
      } else {

         mUnreadReplies[foundId] = reply;
      }
   }
   return found;
}

/**
 * Clean up pending sends/replies that are older than 5 minutes 
 */
void BoomStick::CleanOldPendingData() {
   const auto unreadSize = mUnreadReplies.size();
   const auto pendingSize = mPendingReplies.size();

   if (!mUnreadAlert && unreadSize >= mUnreadAlertSize) {
      mUnreadAlert = true;
      LOG(WARNING) << "unread commands has exceeded " << mUnreadAlertSize;
   } else if (mUnreadAlert && unreadSize < mUnreadAlertSize) {
      mUnreadAlert = false;
      LOG(INFO) << "unread commands has dropped back below our max size " << mUnreadAlertSize;
   }

   if (!mPendingAlert && pendingSize >= mPendingAlertSize) {
      mPendingAlert = true;
      LOG(WARNING) << "pending commands has exceeded " << mPendingAlertSize;
   } else if (mPendingAlert && pendingSize < mPendingAlertSize) {

      mPendingAlert = false;
      LOG(INFO) << "pending commands has dropped back below our max size " << mPendingAlertSize;
   }
   CleanUnreadReplies();
   CleanPendingReplies();
}

/**
 * Cleanup pending replies that have exceeded our timeout.
 */
void BoomStick::CleanPendingReplies() {
   time_t now = time(NULL);
   if (now < mLastGCTime + 5 * MINUTES_TO_SECONDS) {
      return;
   }
   mLastGCTime = now;
   std::vector<std::string> uuidsToRemove;
   for (auto pendingSend : mPendingReplies) {
      if (pendingSend.second <= now - 5 * MINUTES_TO_SECONDS) {
         LOG(DEBUG) << "Removed Pending Reply for " << pendingSend.first;
         uuidsToRemove.push_back(pendingSend.first);
      }
   }
   int deleteUnread = 0;
   for (auto uuid : uuidsToRemove) {
      mPendingReplies.erase(uuid);
      if (mUnreadReplies.find(uuid) != mUnreadReplies.end()) {

         deleteUnread++;
         mUnreadReplies.erase(uuid);
      }
   }
   LOG_IF(INFO, (deleteUnread > 0)) << "Deleted " << deleteUnread << " unread replies that exceed the 5 minute timeout";
   ShrinkToFit(mPendingReplies);
   ShrinkToFit(mUnreadReplies);

}

/**
 * Cleanup unread replies that don't exist in pending.
 */
void BoomStick::CleanUnreadReplies() {
   std::vector<std::string> uuidsToRemove;
   //check all unread replies and remove any that don't exist in pending.
   for (auto unreadReply : mUnreadReplies) {
      if (mPendingReplies.find(unreadReply.first) == mPendingReplies.end()) {
         LOG(WARNING) << "Found unmatched reply to unknown hash " << unreadReply.first;
         uuidsToRemove.push_back(unreadReply.first);
      }
   }

   int count = 0;
   for (auto hash : uuidsToRemove) {
      count++;
      mUnreadReplies.erase(hash);
   }
   LOG_IF(INFO, (count > 0)) << "Deleted " << count << " replies that no longer exist in pending";

}
