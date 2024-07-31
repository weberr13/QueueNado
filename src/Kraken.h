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
#include <cstdint>

struct _zctx_t;
typedef struct _zctx_t zctx_t;
/** Harpoon-Kraken is a PipeLine communication pattern used to
*  Battling files or plain data from a server to a client. 
* 
* The Client: Here called Harpoon connects (harpoon hit) the 
* Kraken (server).  
* 
* The Harpoon keeps receiving/pulling data from the Kraken until 
* mission achieved (Kraken is dead...) the data is received. 
* 
* Originally this was described at zeromq.org as fileio model:
* http://zguide.zeromq.org/c:fileio3
*/
class Kraken {
public:

   enum class Spear : std::int8_t { MISS = -1, IMPALED = 0 };
   enum class Battling : std::int8_t { TIMEOUT = -2, INTERRUPT = -1, CONTINUE = 0, CANCEL = 1 };
   typedef std::vector<uint8_t> Chunks;


   Kraken();
   Spear SetLocation(const std::string& location);
   void MaxWaitInMs(const int timeout);
   void ChangeDefaultMaxChunkSizeInBytes(const size_t bytes);
   size_t MaxChunkSizeInBytes();
   Battling FinalBreach();
   Battling SendTidalWave(const Chunks& data);
   virtual ~Kraken();

   std::string EnumToString(Battling type) const;
    
protected:
   
   Battling SendRawData(const uint8_t*, int size);
   Battling PollTimeout(int timeoutMs);
   Battling NextChunkId(); 
   void FreeOldRequests();
   void FreeChunk();

private:
   void* mRouter;
   zctx_t* mCtx;
   std::string mLocation;
   size_t mQueueLength;
   size_t mMaxChunkSize;
   char* mNextChunk;
   zframe_t* mIdentity;
   int mTimeoutMs;
   zframe_t* mChunk;
};
