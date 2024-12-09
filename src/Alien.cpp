#include <memory>
#include "czmq.h"
#include "boost/thread.hpp"
#include "g3log/g3log.hpp"

#include "Alien.h"

/**
 * Alien is a ZeroMQ Sub socket.
 */
Alien::Alien()
{
   mCtx = zmq_ctx_new();
   CHECK(mCtx != nullptr);
   mBody = zmq_socket(mCtx, ZMQ_SUB);
   CHECK(mBody != nullptr);
}

/**
 * Setup the location to receive messages.
 * @param location
 */
void Alien::PrepareToBeShot(const std::string &location)
{
   // Subscribe to everything
   char dummy = '\0';
   int rc = zmq_setsockopt(mBody, ZMQ_SUBSCRIBE, &dummy, 0); // Empty subscription
   if (rc != 0)
   {
      std::cerr << "Failed to subscribe: " << rc << std::endl;
      throw std::string("Failed to subscribe to the socket");
   }
   // Set High Water Marks (HWM)
   int hwm = 32 * 1024;
   rc = zmq_setsockopt(mBody, ZMQ_RCVHWM, &hwm, sizeof(hwm));
   if (rc != 0)
   {
      std::cerr << "Failed to set receive HWM: " << rc << std::endl;
      throw std::string("Failed to set receive HWM");
   }

   rc = zmq_setsockopt(mBody, ZMQ_SNDHWM, &hwm, sizeof(hwm));
   if (rc != 0)
   {
      std::cerr << "Failed to set send HWM: " << rc << std::endl;
      throw std::string("Failed to set send HWM");
   }

   // Connect the socket to the specified location
   rc = zmq_connect(mBody, location.c_str());
   if (rc != 0)
   {
      std::cerr << "Failed to connect to location: " << location << std::endl;
      std::cerr << "Error code: " << rc << std::endl;
      throw std::string("Failed to connect to socket");
   }
}

/**
 * Blocking call that returns when the alien has been shot.
 * @return
 */
std::vector<std::string> Alien::GetShot()
{
   std::vector<std::string> bullets;
   while (!zctx_interrupted && bullets.empty())
   {
      GetShot(1000, bullets);
      boost::this_thread::interruption_point();
   }
   if (zctx_interrupted)
   {
      LOG(INFO) << "Caught Interrupt Signal";
   }
   return bullets;
}

/**
 * Blocking call that returns when the alien has been shot.
 * @return
 */
void Alien::GetShot(const unsigned int timeout, std::vector<std::string> &bullets)
{
   bullets.clear();
   if (!mBody)
   {
      LOG(WARNING) << "Alien attempted to GetShot but is not properly initialized";
      return;
   }

   if (zsocket_poll(mBody, timeout))
   {
      zmsg_t *msg = zmsg_recv(mBody);
      if (msg && zmsg_size(msg) >= 2)
      {
         zframe_t *data = zmsg_pop(msg);
         if (data)
         {
            // remove the first frame
            zframe_destroy(&data);
         }
         int msgSize = zmsg_size(msg);
         for (int i = 0; i < msgSize; i++)
         {
            data = zmsg_pop(msg);
            if (data)
            {
               std::string bullet;
               bullet.assign(reinterpret_cast<char *>(zframe_data(data)), zframe_size(data));
               bullets.push_back(bullet);
               zframe_destroy(&data);
            }
         }
      }
      else
      {
         if (msg)
         {
            LOG(WARNING) << "Got Invalid bullet of size: " << zmsg_size(msg);
         }
      }
      if (msg)
      {
         zmsg_destroy(&msg);
      }
   }
}

/**
 * Destroy the body and context of the alien.
 */
Alien::~Alien()
{
   zsocket_destroy(mCtx, mBody);
   zctx_destroy(&mCtx);
}
