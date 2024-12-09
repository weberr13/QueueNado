/*
 * File:   Harpoon.h
 * Author: Ryan Kophs
 *
 * Created on December, 2014
 */

#pragma once
#include <string>
#include <vector>
#include <czmq.h>

/** Harpoon-Kraken is a PipeLine communication pattern used to
 *  stream files or plain data from a server to a client.
 *
 * The Client: Here called Harpoon connects (harpoon hit) the
 * Kraken (server).
 *
 * The Harpoon keeps receiving/pulling data from the Kraken until
 * mission achieved (Kraken is dead...) all the data is received.
 *
 * Originally this was described at zeromq.org as fileio model:
 * http://zguide.zeromq.org/c:fileio3
 */
class Harpoon
{
public:
   enum class Spear : std::int8_t
   {
      MISS = -1,
      IMPALED = 0
   };
   enum class Battling : std::int8_t
   {
      TIMEOUT = -2,
      INTERRUPT = -1,
      VICTORIOUS = 0,
      CONTINUE = 1,
      CANCEL = 2
   };

   Harpoon();

   Spear Aim(const std::string &location);
   void MaxWaitInMs(const int timeoutMs);
   Battling Heave(std::vector<uint8_t> &data);
   Battling Cancel();
   virtual ~Harpoon();

   std::string EnumToString(Battling type) const;

protected:
   Battling PollTimeout(int timeoutMs);
   void RequestChunks();
   void FreeChunk();

private:
   void* mDealer;
   zmq_ctx_t* mCtx;
   size_t mQueueLength;
   int mTimeoutMs;
   size_t mCredit;
   size_t mOffset;
   zframe_t *mChunk;
};
