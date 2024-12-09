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
   zsocket_set_subscribe(mBody, &dummy);
   zsocket_set_rcvhwm(mBody, 32 * 1024);
   zsocket_set_sndhwm(mBody, 32 * 1024);
   int rc = zsocket_connect(mBody, location.c_str());
   if (rc == -1)
   {
      LOG(WARNING) << "location: " << location;
      LOG(WARNING) << "connect socket rc == " << rc;
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
