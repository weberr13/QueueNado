#define _OPEN_SYS
#include <sys/stat.h>
#include <zmq.h>

#include "Rifle.h"
#include "czmq.h"
#include "g3log/g3log.hpp"
#include "Death.h"
/**
 * Construct our Rifle which is a push in our ZMQ push pull.
 */
Rifle::Rifle(const std::string& location) :
mLocation(location),
mHwm(500),
mChamber(NULL),
mContext(NULL),
mLinger(10),
mIOThredCount(1),
mOwnSocket(true) {
}

/**
 * Return thet location we are going to be shot.
 * @return 
 */
std::string Rifle::GetBinding() const {
   return mLocation;
}

/**
 * Get our high water mark.
 * @return 
 */
int Rifle::GetHighWater() {
   return mHwm;
}

/**
 * Set our highwatermark. This must be called before Aim.
 * @param hwm
 */
void Rifle::SetHighWater(const int hwm) {
   mHwm = hwm;
}

/**
 * Set IO thread count. This must be called before Aim.
 * @param count
 */
void Rifle::SetIOThreads(const int count) {
   mIOThredCount = count;
}

/**
 * Get IO thread count;
 * @param count
 */
int Rifle::GetIOThreads() {
   return mIOThredCount;
}

/**
 * Set if we own the socket or not, if we do bind to it.
 * @param own
 */
void Rifle::SetOwnSocket(const bool own) {
   mOwnSocket = own;
}

/**
 * Get value for owning the socket.
 * @return bool
 */
bool Rifle::GetOwnSocket() {
   return mOwnSocket;
}

/**
 * Set the location we want to shoot at.
 * @param location
 * @return 
 */
bool Rifle::Aim() {
   if (mChamber) {
      return true;
   }
   if (!mContext) {
      mContext = zmq_ctx_new();
      zmq_ctx_set(mContext, ZMQ_LINGER, 0);   // linger for a millisecond on close
      zmq_ctx_set(mContext, ZMQ_SNDHWM, GetHighWater());
      zmq_ctx_set(mContext, ZMQ_RCVHWM, GetHighWater()); // HWM on internal thread communication
      zmq_ctx_set(mContext, ZMQ_IO_THREADS, 1);  
   }
   if (!mChamber) {
      mChamber = zmq_socket(mcontext, ZMQ_PUSH);
      CZMQToolkit::setHWMAndBuffer(mChamber, GetHighWater());
      if (GetOwnSocket()) {
         int result = zmq_bind(mChamber, mLocation.c_str());

         if (result < 0) {
            LOG(WARNING) << "Rifle can't bind : " << result;
            zmq_close(mChamber);
            zmq_ctx_destroy(mContext);
            mChamber = NULL;
            return false;
         }
         setIpcFilePermissions();
         Death::Instance().RegisterDeathEvent(&Death::DeleteIpcFiles, mLocation);
      } else {
         int result = zmq_connect(mChamber, mLocation.c_str());
         if (result < 0) {
            LOG(WARNING) << "Rifle can't connect : " << result;
            zmq_close(mChamber);
            zmq_ctx_destroy(mContext);
            mChamber = NULL;
            return false;
         }
      }
      //CZMQToolkit::PrintCurrentHighWater(mChamber, "Rifle: chamber");
   }
   return ((mContext != NULL) && (mChamber != NULL));

}

/**
 * Set the file permisions on an IPC socket to 0777
 */
void Rifle::setIpcFilePermissions() {

   mode_t mode = S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IWGRP
      | S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH;

   size_t ipcFound = mLocation.find("ipc");
   if (ipcFound != std::string::npos) {
      size_t tmpFound = mLocation.find("/tmp");
      if (tmpFound != std::string::npos) {
         std::string ipcFile = mLocation.substr(tmpFound);
         LOG(INFO) << "Rifle set ipc permissions: " << ipcFile;
         chmod(ipcFile.c_str(), mode);
      }
   }
}

/**
 * Shoot a buillet / message to the Vampires / pull.
 *
 * @param bullet
 *
 * @param waitToFire in milliseconds
 *
 * @return 
 */
bool Rifle::Fire(const std::string& bullet, const int waitToFire) {
   //LOG(DEBUG) << "RifleFire";
   if (!mChamber) {
      LOG(WARNING) << "Socket uninitialized!";
      return false;
   }
   if (bullet.empty()) {
      LOG(WARNING) << "Tried to send empty packet";
      return false;
   }
   zmq_pollitem_t items [] = {
      { mChamber, 0, ZMQ_POLLOUT, 0}
   };

   if (zmq_poll(items, 1, waitToFire) > 0) {
      if (items[0].revents & ZMQ_POLLOUT) {
         zmsg_t* message = zmsg_new();
         zmsg_addmem(message, &(bullet[0]), bullet.size());
         return CZMQToolkit::SendExistingMessage(message, mChamber);
      } else {
         LOG(WARNING) << "Error on Zmq socket send: " << zmq_strerror(zmq_errno());
         return false;
      }
   } else {
      //      LOG(WARNING) << "timeout in zmq_pollout " << GetBinding();
      return false;
   }
}

/**
 * Fire a string without copying it to zeromq. 
 * @param zero
 * @param size
 * @param FreeFunction
 * @param waitToFire
 * @return 
 */
bool Rifle::FireZeroCopy(std::string* zero, const size_t size, void (*FreeFunction)(void*, void*), const int waitToFire) {
   bool success = false;
   if (!mChamber) {
      LOG(WARNING) << "Socket uninitialized!";
   } else if (size == 0) {
      LOG(WARNING) << "Tried to send empty packet";
   } else {
      zmq_pollitem_t items [] = {
         { mChamber, 0, ZMQ_POLLOUT, 0}
      };

      if (zmq_poll(items, 1, waitToFire) > 0) {
         if (items[0].revents & ZMQ_POLLOUT) {

            zmq_msg_t message;
            zmq_msg_init_data(&message, &((*zero)[0]), size, FreeFunction, zero);
            if ((int) size == zmq_msg_send(&message, mChamber, ZMQ_DONTWAIT)) {
               success = true;
               zero = NULL;
            }
         } else {
            LOG(WARNING) << "Error on Zmq socket send: " << zmq_strerror(zmq_errno());
         }
      } else {
         //      LOG(WARNING) << "timeout in zmq_pollout " << GetBinding();
      }
   }
   if (!success && zero) {
      delete zero;
      zero = NULL;
   }
   return success;
}

/**
 * Shoot a pointer / message to the Vampires / pull.
 * @param stake
 * @return 
 */
bool Rifle::FireStake(const void* stake, const int waitToFire) {
   if (!mChamber) {
      LOG(WARNING) << "Socket uninitialized!";
      return false;
   }
   if (stake == NULL) {
      LOG(WARNING) << "Tried to send empty packet";
      return false;
   }
   zmq_pollitem_t items [] = {
      { mChamber, 0, ZMQ_POLLOUT, 0}
   };

   if (zmq_poll(items, 1, waitToFire) > 0) {
      if (items[0].revents & ZMQ_POLLOUT) {
         zmsg_t* message = zmsg_new();
         zmsg_addmem(message, &(stake), sizeof (void*));
         return CZMQToolkit::SendExistingMessage(message, mChamber);
      } else {

         LOG(WARNING) << "Error in zmq_pollout in " << GetBinding() << ": " << zmq_strerror(zmq_errno());
         return false;
      }
   } else {
      //      LOG(WARNING) << "timeout in zmq_pollout " << GetBinding();
      return false;
   }
}

/**
 * Shoot a vector of pointers and some sort of hash message to the Vampires / pull.
 * @param stakes
 *   A vector of pairs, first being a pointer that the sender gives ownership
 * of, and a has of the data associated with the pointer
 * @return 
 *   false if something went wrong
 */
bool Rifle::FireStakes(const std::vector<std::pair<void*, unsigned int> >
   & stakes, const int waitToFire) {
   bool success = false;
   if (!mChamber) {
      LOG(WARNING) << "Socket uninitialized!";
   } else if (stakes.empty()) {
      LOG(WARNING) << "Tried to send nothing";
   } else {
      zmq_pollitem_t items [] = {
         { mChamber, 0, ZMQ_POLLOUT, 0}
      };

      if (zmq_poll(items, 1, waitToFire) > 0) {
         if (items[0].revents & ZMQ_POLLOUT) {
            zmsg_t* message = zmsg_new();
            zmsg_addmem(message, &(stakes[0]),
               stakes.size() * (sizeof (std::pair<void*, unsigned int>)));
            success = CZMQToolkit::SendExistingMessage(message, mChamber);
         } else {
            LOG(WARNING) << "Error in zmq_pollout in " << GetBinding() << ": " << zmq_strerror(zmq_errno());
         }
      } else {
         //      LOG(WARNING) << "timeout in zmq_pollout " << GetBinding();
      }
   }
   return success;
}

/**
 * Destroy the gun.
 */
void Rifle::Destroy() {
   if (mContext != NULL) {
      //LOG(DEBUG) << "Rifle: destroying context";
      zmq_close(mChamber);
      zmq_ctx_destroy(mContext);
      zmq_ctx_destroy(&mContext);
      //zclock_sleep(mLinger * 2);
      mChamber = NULL;
      mContext = NULL;
   }
}

Rifle::~Rifle() {
   Destroy();
}
