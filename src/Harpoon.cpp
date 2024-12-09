/*
 * File:   Harpoon.h
 * Author: Ryan Kophs
 *
 * Created on December, 2014
 */

#include <czmq.h>
#include <g3log/g3log.hpp>
#include <algorithm>
#include "Harpoon.h"
#include <chrono>
#include <zmq.h>
#include <zsock.h>

/// Creates the client that is to connect to the server/Kraken
Harpoon::Harpoon() : mQueueLength(1),    // Number of allowed messages in queue
                     mTimeoutMs(300000), // 5 minutes
                     mOffset(0),
                     mChunk(nullptr)
{
   mCtx = zctx_new();
   CHECK(mCtx != nullptr);
   mDealer = zsocket_new(mCtx, ZMQ_DEALER);
   CHECK(mDealer != nullptr);
   mCredit = mQueueLength;
}

/// Set location of the queue (TCP location)
Harpoon::Spear Harpoon::Aim(const std::string &location)
{
   int result = zsocket_connect(mDealer, location.c_str());
   return (0 == result) ? Harpoon::Spear::IMPALED : Harpoon::Spear::MISS;
}

/// Set the amount of time in MS the client should wait for new data.
void Harpoon::MaxWaitInMs(const int timeoutMs)
{
   mTimeoutMs = timeoutMs;
}

/// Send out ACKSs to the Server that request new chunks. The server will only fill up the
/// queue with a number of responses equal to the number of ACKs in the queue in order
/// to ensure the queue doesn't get overloaded. Max around of chunks is equal to mCredit
/// default to 10 max.
void Harpoon::RequestChunks()
{
   // Send enough data requests to fill pipeline:
   while (mCredit && !zctx_interrupted)
   {
      zstr_sendf(mDealer, "%ld", mOffset);
      mOffset++;
      mCredit--;
   }
}

Harpoon::Battling Harpoon::Cancel()
{
   FreeChunk();
   RequestChunks();
   zstr_sendf(mDealer, EnumToString(Harpoon::Battling::CANCEL).c_str());
   std::vector<uint8_t> ignored;
   return Harpoon::Heave(ignored);
}

// Wait for input on the queue
//  NOTE: zsocket_poll returns true only when the ZMQ_POLLIN is returned by zmq_poll. If false is
//  returned it does not automatically mean a timeout occurred waiting for input. So std::chrono is
//  used to determine when the poll has truly timed out.
Harpoon::Battling Harpoon::PollTimeout(int timeoutMs)
{
   using namespace std::chrono;

   steady_clock::time_point pollStartMs = steady_clock::now();
   while (!zsocket_poll(mDealer, 1))
   {
      int pollElapsedMs = duration_cast<milliseconds>(steady_clock::now() - pollStartMs).count();
      if (pollElapsedMs >= timeoutMs)
      {
         return Harpoon::Battling::TIMEOUT;
      }
   }
   return Harpoon::Battling::CONTINUE;
}

/// Block until timeout or if there is new data to be received.
Harpoon::Battling Harpoon::Heave(std::vector<uint8_t> &data)
{
   // Erase any previous data from the last Monitor()
   FreeChunk();
   RequestChunks();
   static const std::vector<uint8_t> emptyOnError;

   // Poll to see if anything is available on the pipeline:
   if (Harpoon::Battling::CONTINUE == PollTimeout(mTimeoutMs))
   {

      mChunk = zframe_recv(mDealer);
      if (!mChunk)
      {
         data = emptyOnError;
         return Harpoon::Battling::INTERRUPT;
      }

      int size = zframe_size(mChunk);
      data.resize(size);
      if (size <= 0)
      {
         return Harpoon::Battling::VICTORIOUS;
      }

      uint8_t *raw = reinterpret_cast<uint8_t *>(zframe_data(mChunk));
      std::copy(raw, raw + size, data.begin());

      mCredit++;
      return Harpoon::Battling::CONTINUE;
   }

   data = emptyOnError;
   return Harpoon::Battling::TIMEOUT;
}

/// Free the chunk of data struct used by ZMQ
void Harpoon::FreeChunk()
{
   if (mChunk != nullptr)
   {
      zframe_destroy(&mChunk);
      mChunk = nullptr;
   }
}

/// Destruction and frees of internal zmq memory
Harpoon::~Harpoon()
{
   FreeChunk();
   zsocket_destroy(mCtx, mDealer);
   zctx_destroy(&mCtx);
}

std::string Harpoon::EnumToString(Harpoon::Battling value) const
{
   std::string result;

   switch (value)
   {
   case Harpoon::Battling::TIMEOUT:
      result = "<TIMEOUT>";
      break;
   case Harpoon::Battling::INTERRUPT:
      result = "<INTERRUPT>";
      break;
   case Harpoon::Battling::VICTORIOUS:
      result = "<VICTORIOUS>";
      break;
   case Harpoon::Battling::CONTINUE:
      result = "<CONTINUE>";
      break;
   case Harpoon::Battling::CANCEL:
      result = "<CANCEL>";
      break;
   default:
      result = "UNKNOWN: " + std::to_string(static_cast<int>(value));
   }
   return result;
}
