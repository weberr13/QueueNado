#include <zmq.h>
#include <zlib.h>
#include <czmq.h>
#include <zframe.h>

#include "Crowbar.h"
#include <boost/thread.hpp>
#include <g3log/g3log.hpp>

/**
 * Construct a crowbar for beating things at the binding location
 *
 * @param binding
 *   A std::string description of a ZMQ socket
 */
Crowbar::Crowbar(const std::string &binding) : mContext(NULL),
                                               mBinding(binding), mTip(NULL), mOwnsContext(true)
{
}

/**
 * Construct a crowbar for beating the specific headcrab
 *
 * @param target
 *   A living(initialized) headcrab
 */
Crowbar::Crowbar(const Headcrab &target) : mContext(target.GetContext()),
                                           mBinding(target.GetBinding()), mTip(NULL), mOwnsContext(false)
{
   if (mContext == NULL)
   {
      mOwnsContext = true;
   }
}

/**
 * Construct a crowbar for beating things at binding with the given context
 * @param binding
 *   The binding of the bound socket for the given context
 * @param context
 *   A working context
 */
Crowbar::Crowbar(const std::string &binding, zctx_t *context) : mContext(context),
                                                                mBinding(binding), mTip(NULL), mOwnsContext(false)
{
}

/**
 * Default deconstructor
 */
Crowbar::~Crowbar()
{
   if (mOwnsContext && mContext != NULL)
   {
      zmq_ctx_destroy(&mContext);
   }
}

/**
 * Get the high water mark for socket sends
 *
 * @return
 *   the high water mark
 */
int Crowbar::GetHighWater()
{
   return 1024;
}

/**
 * Get the "tip" socket used to hit things
 *
 * @return
 *   A pointer to a zmq socket (or NULL in a failure)
 */
void *Crowbar::GetTip()
{
   void *tip = zmq_socket(mContext, ZMQ_REQ);
   if (!tip)
   {
      return NULL;
   }

   int high_water_mark = GetHighWater();
   int result = zmq_setsockopt(tip, ZMQ_SNDHWM, &high_water_mark, sizeof(high_water_mark));
   if (result != 0)
   {
      LOG(WARNING) << "Failed to set send high water mark: " << zmq_strerror(zmq_errno());
      zmq_close(tip);
      return NULL;
   }

   result = zmq_setsockopt(tip, ZMQ_RCVHWM, &high_water_mark, sizeof(high_water_mark));
   if (result != 0)
   {
      LOG(WARNING) << "Failed to set send high water mark: " << zmq_strerror(zmq_errno());
      zmq_close(tip);
      return NULL;
   }

   int linger = 0;
   zmq_setsockopt(tip, ZMQ_LINGER, &linger, sizeof(linger));
   int connectRetries = 100;

   while (zmq_connect(tip, mBinding.c_str()) != 0 && connectRetries-- > 0 && !zctx_interrupted)
   {
      boost::this_thread::interruption_point();
      int err = zmq_errno();
      if (err == ETERM)
      {
         zmq_close(tip);
         zmq_ctx_destroy(mContext);

         return NULL;
      }
      std::string error(zmq_strerror(err));
      LOG(WARNING) << "Could not connect to " << mBinding << ": " << error;
      zmq_sleep(100); // zmq_sleep is a replacement for zclock_sleep
   }

   if (zctx_interrupted)
   {
      LOG(INFO) << "Caught Interrupt Signal";
   }
   if (connectRetries <= 0)
   {
      zmq_close(tip);
      zmq_ctx_destroy(mContext);
      return NULL;
   }

   return tip;
}

bool Crowbar::Wield()
{
   if (!mContext)
   {
      mContext = zmq_ctx_new();                              // create a new context
      zmq_ctx_set(mContext, ZMQ_LINGER, 0);              // linger for a millisecond on close
      zmq_ctx_set(mContext, ZMQ_SNDHWM, GetHighWater()); // set send high water mark
      zmq_ctx_set(mContext, ZMQ_RCVHWM, GetHighWater()); // set receive high water mark
      zmq_ctx_set(mContext, ZMQ_IO_THREADS, 1);              // set number of IO threads
   }

   if (!mTip)
   {
      mTip = GetTip();
      if (!mTip && mOwnsContext)
      {
         zmq_ctx_term(mContext); // terminate the context
         mContext = NULL;
      }
   }

   return ((mContext != NULL) && (mTip != NULL));
}

bool Crowbar::Swing(const std::string &hit)
{
   // std::cout << "sending " << hit << std::endl;
   std::vector<std::string> hits;
   hits.push_back(hit);
   return Flurry(hits);
}

/**
 * Poll to see if the other side of the socket is ready
 * @return
 */
bool Crowbar::PollForReady()
{
   zmq_pollitem_t item;
   if (!mTip)
   {
      return false;
   }
   item.socket = mTip;
   item.events = ZMQ_POLLOUT;
   int returnVal = zmq_poll(&item, 1, 0);
   if (returnVal < 0)
   {
      LOG(WARNING) << "Socket error: " << zmq_strerror(zmq_errno());
   }

   return (returnVal >= 1);
}

/**
 * Send a bunch of strings to a socket
 * @param hits
 * @return
 */
bool Crowbar::Flurry(std::vector<std::string> &hits)
{
   if (!mTip)
   {
      LOG(WARNING) << "Cannot send, not Wielded";
      return false;
   }
   if (!PollForReady())
   {
      LOG(WARNING) << "Cannot send, no listener ready";
      return false;
   }
   zmsg_t *message = zmsg_new();
   for (auto it = hits.begin();
        it != hits.end(); it++)
   {
      zmsg_addmem(message, &((*it)[0]), it->size());
   }
   bool success = true;
   // std::cout << "Sending message with " << zmsg_size(message) << " " << hits.size() << std::endl;
   if (zmsg_send(&message, mTip) != 0)
   {
      LOG(WARNING) << "zmsg_send returned non-zero exit " << zmq_strerror(zmq_errno());
      success = false;
   }
   if (message)
   {
      zmsg_destroy(&message);
   }
   return success;
}

bool Crowbar::BlockForKill(std::string &guts)
{
   std::vector<std::string> allReplies;
   if (BlockForKill(allReplies) && !allReplies.empty())
   {
      guts = allReplies[0];
      return true;
   }
   return false;
}

bool Crowbar::BlockForKill(std::vector<std::string> &guts)
{
   if (!mTip)
   {
      return false;
   }
   zmsg_t *message = zmsg_recv(mTip);
   if (!message)
   {
      return false;
   }
   guts.clear();
   int msgSize = zmsg_size(message);
   for (int i = 0; i < msgSize; i++)
   {
      zframe_t *frame = zmsg_pop(message);
      std::string aString;
      aString.insert(0, reinterpret_cast<const char *>(zframe_data(frame)), zframe_size(frame));
      guts.push_back(aString);
      zframe_destroy(&frame);
      // std::cout << guts[0] << " found " << aString << std::endl;
   }

   zmsg_destroy(&message);
   return true;
}

bool Crowbar::WaitForKill(std::string &guts, const int timeout)
{
   std::vector<std::string> allReplies;
   if (WaitForKill(allReplies, timeout) && !allReplies.empty())
   {
      guts = allReplies[0];
      return true;
   }
   return false;
}

bool Crowbar::WaitForKill(std::vector<std::string> &guts, const int timeout)
{
   if (!mTip)
   {
      return false;
   }

   // Set up the polling item for mTip socket
   zmq_pollitem_t poll_items[] = {
       {static_cast<void *>(mTip), 0, ZMQ_POLLIN, 0} // Socket, file descriptor, event (ZMQ_POLLIN means incoming message)
   };

   // zmq_poll will block up to the timeout value (in milliseconds)
   int rc = zmq_poll(poll_items, 1, timeout); // Poll one item

   if (rc == -1)
   {
      // Handle poll error
      return false;
   }

   if (poll_items[0].revents & ZMQ_POLLIN)
   {
      // Data is available, proceed to process the message
      return BlockForKill(guts);
   }
   return false;
}

void *Crowbar::GetContext()
{
   return mContext;
}
