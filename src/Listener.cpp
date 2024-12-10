/*
 * File: Listener.cpp
 * Author: Craig Cogdill
 * Created: August 14, 2015 1:08PM
 */

#include <memory>
#include <g3log/g3log.hpp>
#include <thread>
#include <sstream>
#include "Listener.h"
#include <Alien.h>
#include <Rifle.h>

namespace {

/**
* RAII delete the string after it has been sent through the Rifle/Vampire queue back to the 
* Notifier. This can't be a member function of Listener.
*/
void ZeroCopyDelete(void*, void* data) {
  std::string* theString = reinterpret_cast<std::string*>(data);
  delete theString;  
}

} // namespace

std::unique_ptr<Listener>  Listener::CreateListener(const std::string& notificationQueue, const std::string& handshakeQueue, const std::string& program) {
   auto listener = std::unique_ptr<Listener>(new Listener(notificationQueue, handshakeQueue, program));
   if (listener->Initialize()) {
      return listener;
   }
   std::unique_ptr<Listener> deadListener;
   return deadListener;
}

/// destructor
 Listener::~Listener()
 { 
   Reset(); 
};

Listener::Listener(const std::string& notificationQueue, const std::string& handshakeQueue, const std::string& program)
   : mNotificationQueueName(notificationQueue),
     mHandshakeQueueName(handshakeQueue),
     mProgramName(program) {}

/*
 * Initialize a new Alien to point to the
 *    given ipc file. This will enable a
 *    Listener instance to receive
 *    messages from the associated Notifier Shotgun
 * 
 * @return a bool if successful
 */
bool Listener::Initialize() {
   if (mQueueReader.get() == nullptr) {
      mHandshakeQueue = std::move(CreateFeedbackShooter());
      if (!mHandshakeQueue) {
         LOG(WARNING) << "Could not initialize Listener Handshake queue.";
         return false;
      }

      mQueueReader.reset(new Alien);
      try {
         mQueueReader->PrepareToBeShot(mNotificationQueueName);
      } catch (std::exception& e) {
         LOG(WARNING) << "Listener Caught exception: " << e.what();
         Reset();
      }
   }
   return (mQueueReader.get() != nullptr && mHandshakeQueue.get() != nullptr);
}

/*
* Create a Rifle to fire a confirmation message
*     back to the original Notifier
*
* @return a feedback shooter of type Rifle*
*/  
std::unique_ptr<Rifle> Listener::CreateFeedbackShooter() {
   LOG(INFO) << "Creating feedback handshake " << mHandshakeQueueName 
             << " for thread #" << std::this_thread::get_id();
   const size_t feedbackQSize = 100;
   auto shooter = std::unique_ptr<Rifle>(new Rifle(mHandshakeQueueName));
   shooter->SetOwnSocket(false);
   shooter->SetHighWater(feedbackQSize);
   if (!shooter->Aim()) {
      LOG(WARNING) << "Failed to initialize feedback queue : " << mHandshakeQueueName;
      shooter.reset();
   }
   return shooter;
}

/*
 * Reset the member Alien to nullptr
 */
void Listener::Reset() {
   mQueueReader.reset(nullptr);
}

/*
* Signal back to the Notifier that the notification
* was received successfully
*
* @return bool, whether we fired the message successfully
*/
bool Listener::SendConfirmation() {
   CHECK(mHandshakeQueue.get() != nullptr);
   std::ostringstream oss;
   auto id = ThreadID();
   oss << id << " : " << this->mProgramName;
   std::string* pMsg = new std::string(oss.str());
   const bool confirmation = mHandshakeQueue->FireZeroCopy(pMsg, pMsg->size(), ZeroCopyDelete, kBlockForOneMinute);
   if (confirmation) {
      LOG(INFO) << "Send update confirmation, thread #" << oss.str();
   } else {
      LOG(WARNING) << "Failed to send update confirmation, thread # " << oss.str();
   }
   return confirmation;
}

/*
*  Get the ID number of the current thread
*
* @return string
*/
std::string Listener::ThreadID()  {
   std::ostringstream oss;
   oss << std::this_thread::get_id();
   return oss.str();
}

/*
 * The Alien attempts to read a message from
 * the queue. It checks to see if it received
 * anything and returns true if it got the
 * notification message
 *
 * @return true if notification message was received
 */
bool Listener::NotificationReceived() {
   std::vector<std::string> dataFromQueue;
   mQueueReader->GetShot(getShotTimeout, dataFromQueue);
   bool notificationReceived = MessageHasPayload(dataFromQueue);
   if (notificationReceived) {
      ClearMessages();
      StorePayloadIfNecessary(dataFromQueue);
   }
   return notificationReceived;
}

/*
* If the data comes in a specific way, save the vector, as
* it contains messages from the notifier
*
*  @param vector of strings pulled off of the queue
*/
void Listener::StorePayloadIfNecessary(std::vector<std::string>& dataFromQueue) {
   if (dataFromQueue.size() > 1 && dataFromQueue[1] != "notify") {
      RemoveFirstDummyShot(dataFromQueue);
      mMessages = dataFromQueue;
   }
}

/*
* The notifier puts a dummy message at the front of the
* queue. Strip this message from the vector of strings
*
* @param vector of strings where the first string is "notify"
*/
void Listener::RemoveFirstDummyShot(std::vector<std::string>& data) {
   data.erase(data.begin());
}


/*
 * Checks the message received from the queue
 * for the necessary criteria to determine if
 * it received a LuaNotifier notification.
 *
 * @param bool 
 *    A vector of strings is pulled off
 *    of the queue. If the correct message was
 *    seen, shots will contain 2 messages, the
 *    first one being "dummy" and the second
 *    being "notify"
 */
bool Listener::MessageHasPayload(const std::vector<std::string>& shots) {
   return (shots.size() > 0 && !shots[1].empty());
}
