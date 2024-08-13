/*
 * File:   Harpoon.h
 * Author: Ryan Kophs
 *
 * Created on December, 2014
 */

#include <czmq.h>
#include <g3log/g3log.hpp>
#include "Kraken.h"
#include <chrono>

namespace {
   const size_t kDefaultMaxChunkSize_10MB_inBytes = 10 * 1024 * 1024;
}
/// Constructing the server/Kraken that is about to be connected/impaled by the client/Harpoon
Kraken::Kraken():
   mLocation(""),
   mQueueLength(1), //Number of allowed messages in queue
   mMaxChunkSize(kDefaultMaxChunkSize_10MB_inBytes), //10MB
   mNextChunk(nullptr),
   mIdentity(nullptr),
   mTimeoutMs(300000), //5 Minutes
   mChunk(nullptr) {
   mCtx = zctx_new();
   CHECK(mCtx!=nullptr);
   mRouter = zsocket_new(mCtx, ZMQ_ROUTER);
   CHECK(mRouter!=nullptr);
}

/// Set location of the queue (TCP location)
Kraken::Spear Kraken::SetLocation(const std::string& location) {
   mLocation = location;
   zsocket_set_hwm(mRouter, mQueueLength * 2);

   int result = zsocket_bind(mRouter, mLocation.c_str());

   LOG(INFO) << "zsocket_bind result: " << result << ", " << location;
   return (-1 == result) ? Kraken::Spear::MISS : Kraken::Spear::IMPALED;
}

/// Set the amount of time in MS the server should wait for client ACKs
void Kraken::MaxWaitInMs(const int timeoutMs) {
   mTimeoutMs = timeoutMs;
}

/// @param the new default chunk size
void Kraken::ChangeDefaultMaxChunkSizeInBytes(const size_t bytes) {
   mMaxChunkSize = bytes;
}

/// @return max chunk size
size_t Kraken::MaxChunkSizeInBytes() {
   return mMaxChunkSize;
}


//Free the chunk of data struct used by ZMQ in ACKs from the client
void Kraken::FreeOldRequests() {
   if (mIdentity != nullptr) {
      zframe_destroy(&mIdentity);
      mIdentity = nullptr;
   }
   if (mNextChunk != nullptr) {
      delete [] mNextChunk;
      mNextChunk = nullptr;
   }
}

//Free the chunk of data struct used by ZMQ
void Kraken::FreeChunk() {
   if (mChunk != nullptr) {
      zframe_destroy(&mChunk);
      mChunk = nullptr;
   }
}

//Wait for input on the queue
// NOTE: zsocket_poll returns true only when the ZMQ_POLLIN is returned by zmq_poll. If false is
// returned it does not automatically mean a timeout occurred waiting for input. So std::chrono is
// used to determine when the poll has truly timed out.
Kraken::Battling Kraken::PollTimeout(int timeoutMs) {
   using namespace std::chrono;

   steady_clock::time_point pollStartMs = steady_clock::now();
   while (!zsocket_poll(mRouter, 1)) {
      int pollElapsedMs = duration_cast<milliseconds>(steady_clock::now() - pollStartMs).count();
      if (pollElapsedMs >= timeoutMs) {
         return Kraken::Battling::TIMEOUT;
      }
   }
   return Kraken::Battling::CONTINUE;
}

/// Internally used to get an ACK from the client asking for another chunk.
/// We use this so that the sender does not send more data to the client than what the
/// client can consume and therefore overloading the queue.
Kraken::Battling Kraken::NextChunkId() {

   FreeChunk();
   FreeOldRequests();

   //Poll to see if anything is available on the pipeline:
   if (Kraken::Battling::CONTINUE == PollTimeout(mTimeoutMs)) {

      // First frame is the identity of the client
      mIdentity = zframe_recv (mRouter);
      if (!mIdentity) {
         return Kraken::Battling::INTERRUPT;
      }

   } else {
      return Kraken::Battling::TIMEOUT;
   }

   //Poll to see if anything is available on the pipeline:
   if (Kraken::Battling::CONTINUE == PollTimeout(mTimeoutMs)) {

      // Second frame is next chunk requested of the file
      mNextChunk = zstr_recv (mRouter);
      static const std::string kCancel = EnumToString(Kraken::Battling::CANCEL);
      if (!mNextChunk) {
         return Kraken::Battling::INTERRUPT;
      } else if (EnumToString(Kraken::Battling::CANCEL)== mNextChunk) {
         LOG(WARNING) << "Client/Harpoon requested the ongoing transfer to be cancelled";
         return Kraken::Battling::CANCEL;
      }

      return Kraken::Battling::CONTINUE;

   }

   return Kraken::Battling::TIMEOUT;
}

/** Send data to client
* The actual  data might be sent in several small chunks
* if the data size to send is larger than @ref MaxChunkSize()
* @param dataToSend
* @return status of the send operation
*/
Kraken::Battling Kraken::SendTidalWave(const Kraken::Chunks& dataToSend) {
   size_t size = dataToSend.size();
   if (size == 0) {
      return Kraken::Battling::CONTINUE;
   }

   const uint8_t* data = dataToSend.data();
   Kraken::Battling status = Kraken::Battling::CONTINUE;

   for (size_t i = 0; i < size; i += mMaxChunkSize) {
      size_t chunkSize = std::min(size - i, mMaxChunkSize);

      status = SendRawData(&data[i], chunkSize);
      if (Kraken::Battling::CONTINUE != status) {
         return status; // timout, interrupt or cancel
      }
   }

   return status;
}

/// Signals the end of the Battling. This HAS TO BE CALLED by the Client
/// when transfer is finished.
Kraken::Battling Kraken::FinalBreach() {
   auto complete = SendRawData(nullptr, 0);

   //Clean out any previous packets in the channel to avoid memory leaks
   if (Kraken::Battling::CONTINUE == PollTimeout(100)) {
      FreeOldRequests();
      mIdentity = zframe_recv(mRouter);
   }
   return complete;
}

/// Internal call to send a data array to the client.
Kraken::Battling Kraken::SendRawData(const uint8_t* data, int size) {

   FreeChunk();
   FreeOldRequests();
   const auto next = NextChunkId();
   if (Kraken::Battling::CONTINUE != next) {
      return next;
   }

   mChunk = zframe_new(data, size);
   // Send chunk to client
   zframe_send (&mIdentity, mRouter, ZFRAME_REUSE + ZFRAME_MORE);
   zframe_send (&mChunk, mRouter, 0);
   return Kraken::Battling::CONTINUE;

}


/// Destruction of the Kraken and zmq socket and memory cleanup
Kraken::~Kraken() {
   zsocket_unbind(mRouter, mLocation.c_str());
   zsocket_destroy(mCtx, mRouter);
   zctx_destroy(&mCtx);
   mCtx = nullptr;
   FreeOldRequests();
   FreeChunk();
}


std::string Kraken::EnumToString(Battling value) const{
   std::string result;
   switch (value) {
      case Battling::TIMEOUT: result = "<TIMEOUT>"; break;
      case Battling::INTERRUPT: result = "<INTERRUPT>"; break;
      case Battling::CONTINUE: result = "<CONTINUE>"; break;
      case Battling::CANCEL: result = "<CANCEL>"; break;
      default :
         result = "UNKNOWN: " + std::to_string(static_cast<int>(value));
   }
   return result;
}

