#include <memory>
#include "czmq.h"
#include "boost/thread.hpp"
#include "g3log/g3log.hpp"
#include <iostream>
#include <vector>
#include <string>
#include <zmq.h>

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

    // Create a zmq_pollitem_t for the socket
    zmq_pollitem_t item = {mBody, 0, ZMQ_POLLIN, 0};

    // Use zmq_poll to check the socket for messages
    int rc = zmq_poll(&item, 1, timeout);
    if (rc == -1)
    {
        // Handle the error if zmq_poll fails
        LOG(WARNING) << "zmq_poll failed: " << zmq_strerror(zmq_errno());
        return;
    }

    if (item.revents & ZMQ_POLLIN)
    {
        // A message is available, so receive it
        zmq_msg_t msg;
        zmq_msg_init(&msg);

        rc = zmq_msg_recv(&msg, mBody, 0);
        if (rc == -1)
        {
            LOG(WARNING) << "Failed to receive message: " << zmq_strerror(zmq_errno());
            zmq_msg_close(&msg);
            return;
        }

        // Process the message
        size_t msgSize = zmq_msg_size(&msg);
        size_t offset = 0;

        while (offset < msgSize)
        {
            // Create a frame
            zmq_msg_t frame;
            zmq_msg_init_size(&frame, msgSize - offset);

            // Copy data from the message to the frame
            memcpy(zmq_msg_data(&frame), static_cast<char *>(zmq_msg_data(&msg)) + offset, zmq_msg_size(&frame));
            offset += zmq_msg_size(&frame);

            // Extract the data from the frame
            std::string bullet(static_cast<char *>(zmq_msg_data(&frame)), zmq_msg_size(&frame));
            bullets.push_back(bullet);

            zmq_msg_close(&frame);
        }

        zmq_msg_close(&msg);
    }
    else
    {
        // Timeout or no message
        LOG(WARNING) << "No messages received within the timeout period.";
    }
}

/**
 * Destroy the body and context of the alien.
 */
Alien::~Alien()
{
   zmq_close(mBody);      // Close the socket
   zmq_ctx_destroy(mCtx); // Destroy the context
   zmq_ctx_destroy(&mCtx);
}
