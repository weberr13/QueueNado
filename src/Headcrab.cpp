#include <zmq.h>
#include <zlib.h>
#include <czmq.h>
#define _OPEN_SYS
#include <sys/stat.h>

#include "Headcrab.h"
#include "boost/thread.hpp"
#include <g3log/g3log.hpp>
#include "Death.h"


/**
 * Construct a headcrab at the given ZMQ binding
 * 
 * @param binding
 *   A ZeroMQ binding
 */
Headcrab::Headcrab(const std::string& binding) : mBinding(binding), mContext(NULL), mFace(NULL) {

}

/**
 * Default deconstructor
 */
Headcrab::~Headcrab() {
   if (mContext) {
      zmq_ctx_destroy(&mContext);
   }
}

/**
 * Get the high water mark for socket sends
 * 
 * @return 
 *   the high water mark
 */
int Headcrab::GetHighWater() {
   return 1024;
}

/**
 * Populate the internal socket used as the forward facing socket
 * 
 * @param context
 *   The current context
 * @return 
 *   A pointer to the socket (or NULL in the case of a failure)
 */
void* Headcrab::GetFace(void* context) {
   if (mFace == NULL && context) {
      void* face = zmq_socket(context, ZMQ_REP);
      assert(face != NULL);

      int high_water_mark = GetHighWater();

   int result = zmq_setsockopt(face, ZMQ_SNDHWM, &high_water_mark, sizeof(high_water_mark));
   if (result != 0)
   {
      LOG(WARNING) << "Failed to set send high water mark: " << zmq_strerror(zmq_errno());
      zmq_close(face);
      return NULL;
   }

   result = zmq_setsockopt(face, ZMQ_RCVHWM, &high_water_mark, sizeof(high_water_mark));
   if (result != 0)
   {
      LOG(WARNING) << "Failed to set send high water mark: " << zmq_strerror(zmq_errno());
      zmq_close(face);
      return NULL;
   }

   int linger = 0;
   zmq_setsockopt(face, ZMQ_LINGER, &linger, sizeof(linger));

      int connectRetries = 100;
      while (zmq_bind(face, mBinding.c_str()) < 0 && connectRetries-- > 0) {
         boost::this_thread::interruption_point();
         int err = zmq_errno();
         if (err == ETERM) {
            return NULL;
         }
         std::string error(zmq_strerror(err));
         LOG(WARNING) << "Could not bind to " << GetBinding() << ":" << error;

         zclock_sleep(100);
      }
      Death::Instance().RegisterDeathEvent(&Death::DeleteIpcFiles, GetBinding());
      if (connectRetries <= 0) {
         return NULL;
      }
      setIpcFilePermissions();
      mFace = face;
   }
   return mFace;
}

/**
 * Set the file permisions on an IPC socket to 0777
 */
void Headcrab::setIpcFilePermissions() {

   mode_t mode = S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IWGRP
           | S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH;

   std::string binding(GetBinding());
   size_t ipcFound = binding.find("ipc");
   if (ipcFound != std::string::npos) {
      size_t tmpFound = binding.find("/tmp");
      if (tmpFound != std::string::npos) {
         std::string ipcFile = binding.substr(tmpFound);
         LOG(INFO) << "Headcrab set ipc permissions: " << ipcFile;
         chmod(ipcFile.c_str(), mode);
      }
   }
}

/**
 * Initialize internal state, get ready to receive a whacking
 * 
 * @return
 *   If initialization has worked 
 */
bool Headcrab::ComeToLife() {
   if (! mContext) {
      mContext = zmq_ctx_new();
      zmq_ctx_set(mContext, ZMQ_LINGER, 0);   // linger for a millisecond on close
      zmq_ctx_set(mContext, ZMQ_SNDHWM, GetHighWater());
      zmq_ctx_set(mContext, ZMQ_RCVHWM, GetHighWater()); // HWM on internal thread communication
      zmq_ctx_set(mContext, ZMQ_IO_THREADS, 1);  
   }
   if (! mFace) {
      void* face = GetFace(mContext);
      if (! face) {
         return false;
      }
   }

   return ((mContext != NULL) && (mFace != NULL));
}

/**
 * Get the ZMQ socket name that the headcrab would/is bound to
 * @return 
 */
std::string Headcrab::GetBinding() const {
   return mBinding;
}

/**
 * Get the contex
 * @return 
 *   The context if the headcrab is alive, or NULL
 */
void* Headcrab::GetContext() const {
   return mContext;
}

bool Headcrab::GetHitBlock(std::string& theHit) {
   std::vector<std::string> hits;
   if (GetHitBlock(hits) && ! hits.empty()) {
      theHit = hits[0];
      return true;
   }
   return false;
}

bool Headcrab::GetHitBlock(std::vector<std::string>& theHits) {
   if (! mFace) {
      return false;
   }
   zmsg_t* message = zmsg_recv(mFace);
   if (! message) {
      return false;
   }
   //std::cout << "Got message with " << zmsg_size(message) << " parts" << std::endl;
   theHits.clear();
   int msgSize = zmsg_size(message);
   for (int i = 0; i < msgSize; i ++) {
      zframe_t* frame = zmsg_pop(message);
      std::string aFrame;
      aFrame.insert(0, reinterpret_cast<const char*> (zframe_data(frame)), zframe_size(frame));
      theHits.push_back(aFrame);
      zframe_destroy(&frame);
      //std::cout << "got string " << aFrame << " " << theHits[i] << std::endl;
   }

   zmsg_destroy(&message);
   //std::cout << "got " << theHits.size() << " hits" << std::endl;
   return true;

}

bool Headcrab::GetHitWait(std::string& theHit, const int timeout) {
   std::vector<std::string> hits;
   if (GetHitWait(hits, timeout) && ! hits.empty()) {
      theHit = hits[0];
      return true;
   }
   return false;
}

bool Headcrab::GetHitWait(std::vector<std::string>& theHits, const int timeout) {
    if (!mFace) {
        return false; // Return false if the socket is invalid
    }

    // Create a zmq_pollitem_t structure to monitor the socket
    zmq_pollitem_t items[] = {
        { mFace, 0, ZMQ_POLLIN, 0 }  // Watch for incoming data (ZMQ_POLLIN)
    };

    // Call zmq_poll to block or timeout based on the provided 'timeout' in milliseconds
    int rc = zmq_poll(items, 1, timeout);  // Timeout in milliseconds

    if (rc == -1) {
        // Error handling if zmq_poll fails
        return false;
    }

    // Check if the socket has any incoming data (ZMQ_POLLIN)
    if (items[0].revents & ZMQ_POLLIN) {
        // Data is available on the socket, process it
        return GetHitBlock(theHits);
    }

    return false;  // Timeout or no data available
}

bool Headcrab::SendSplatter(const std::string& feedback) {
   std::vector<std::string> allReplies;
   allReplies.push_back(feedback);
   return SendSplatter(allReplies);
}

bool Headcrab::SendSplatter(std::vector<std::string>& feedback) {
   if (! mFace) {
      return false;
   }
   zmsg_t* message = zmsg_new();
   for (auto it = feedback.begin();
           it != feedback.end(); it ++) {
      zmsg_addmem(message, &((*it)[0]), it->size());
   }
   bool success = true;
   if (zmsg_send(&message, mFace) != 0) {
      success = false;
   }
   if (message) {
      zmsg_destroy(&message);
   }
   return success;
}
