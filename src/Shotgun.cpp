#define _OPEN_SYS
#include <sys/stat.h>
#include <zmq.h>

#include "Shotgun.h"
#include "g3log/g3log.hpp"
#include "czmq.h"
#include "Death.h"
/**
 * Shotgun class is a ZeroMQ Publisher.
 */
Shotgun::Shotgun() {
   mCtx = zmq_ctx_new();
   assert(mCtx);
   mGun = zmq_socket(mCtx, ZMQ_PUB);
}

/**
 * Where to fire our messages.
 * @param location
 */
void Shotgun::Aim(const std::string& location) {
   int hwm = 32 * 1024;
   zmq_setsockopt(mGun, ZMQ_SNDHWM, &hwm, sizeof(hwm));
   zmq_setsockopt(mGun, ZMQ_RCVHWM, &hwm, sizeof(hwm));
   int rc = zmq_bind(mGun, location.c_str());
   if (rc == - 1) {

      LOG(WARNING) << "bound socket rc:" << rc << " : location: " << location;
      LOG(WARNING) << zmq_strerror(zmq_errno());

      throw std::string("Failed to connect to bind socket");
   }
   setIpcFilePermissions(location);
   Death::Instance().RegisterDeathEvent(&Death::DeleteIpcFiles, location);
}

/**
 * Set the file permisions on an IPC socket to 0777
 */
void Shotgun::setIpcFilePermissions(const std::string& location) {

   mode_t mode = S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IWGRP
           | S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH;

   size_t ipcFound = location.find("ipc");
   if (ipcFound != std::string::npos) {
      size_t tmpFound = location.find("/tmp");
      if (tmpFound != std::string::npos) {
         std::string ipcFile = location.substr(tmpFound);
         LOG(INFO) << "Shotgun set ipc permissions: " << ipcFile;
         chmod(ipcFile.c_str(), mode);
      }
   }
}

/**
 * Fire our shotgun, hopefully we hit something.
 * @param msg
 */
void Shotgun::Fire(const std::string& bullet) {
   std::vector<std::string> bullets;
   bullets.push_back("dummy");
   bullets.push_back(bullet);
   Fire(bullets);
}

/**
 * Fire our shotgun, hopefully we hit something.
 * @param msg
 */
void Shotgun::Fire(const std::vector<std::string>& bullets) {
   zframe_t* key = zframe_new("key", 0);

   zmsg_t* msg = zmsg_new();
   zmsg_add(msg, key);
   for (auto it = bullets.begin(); it != bullets.end(); it ++) {
      zframe_t* body = zframe_new(it->c_str(), it->size());
      zmsg_add(msg, body);
   }

   if (zmsg_send(&msg, mGun) != 0) {
      LOG(WARNING) << "could not send message";
   }
   if (msg) {
      zmsg_destroy(&msg);
   }
}

/**
 * Cleanup our socket and context.
 */
Shotgun::~ Shotgun() {
   zmq_close(mGun);
   zmq_ctx_destroy(mCtx);
   zmq_ctx_destroy(&mCtx);
}

