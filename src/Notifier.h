/*
 * File: Notifier.h
 * Author: Craig Cogdill
 * Created: August 13, 2015 3:20PM
 */

#pragma once
#include <mutex>
#include <memory>
#include <vector>
#include <string>

class Shotgun;
class Vampire;

class Notifier {
 public:
   static std::unique_ptr<Notifier>  CreateNotifier(const std::string& notifierQueue, const std::string& handshakeQueue, const size_t handshakeCount);
   size_t Notify(const std::vector<std::string>& messages);
   size_t Notify(const std::string& message);
   size_t Notify();
   virtual ~Notifier();

 protected:
   size_t ReceiveConfirmation();
   std::unique_ptr<Vampire> CreateHandshakeQueue();
   void Reset();

 private:
   Notifier() = delete;
   Notifier(const std::string& notifierQueue, const std::string& handshakeQueue);

   bool Initialize(const size_t handshakeCount);

   std::string GetNotifierQueueName();
   std::string GetHandshakeQueueName();
   bool QueuesAreUnitialized();
   const std::string mNotifierQueueName;
   const std::string mHandshakeQueueName;
   std::mutex gLock;
   std::unique_ptr<Shotgun> gQueue;
   std::unique_ptr<Vampire> gHandshakeQueue;
   size_t gHandshakeCount = 0;
   const size_t gMaxTimeoutInSec = 60;
   const std::string kNotifyMessage = "notify";
};
