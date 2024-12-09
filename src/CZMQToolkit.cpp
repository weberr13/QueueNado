#include "CZMQToolkit.h"
#include "g3log/g3log.hpp"
#include <czmq.h>

/**
 * Simple method to set all 4 variables needed to set our buffers and high water mark.
 * @param socket
 * @param size
 */
void CZMQToolkit::setHWMAndBuffer(void *socket, const int size)
{

   //   zmq_setsockopt(socket, ZMQ_SNDHWM, &size,
   //      sizeof (size));
   //   zmq_setsockopt(socket, ZMQ_RCVHWM, &size,
   //      sizeof (size));
   zsock_set_rcvhwm(socket, size);
   //      zsocket_set_rcvbuf(socket, size);
   zsock_set_sndhwm(socket, size);
   //   zsocket_set_sndbuf(socket, size);
}

/**
 * Print the current HWM for the given socket.
 * @param socket
 * @param name
 */
void CZMQToolkit::PrintCurrentHighWater(void *socket, const std::string &name)
{
   int rcvbuf = zsock_rcvbuf(socket);
   int rcvhwm = zsock_rcvhwm(socket);
   int sndbuf = zsock_sndbuf(socket);
   int sndhwm = zsock_sndhwm(socket);
   LOG(DEBUG) << name << ": rcvbuf:" << rcvbuf << " rcvhwm:"
              << rcvhwm << " sndbuf:" << sndbuf << " sndhwm:" << sndhwm;
}

/**
 * Send a message that already exists to a socket
 *
 * @param bullet
 *   A valid message
 * @param socket
 *   An open socket
 * @return
 *   If the call succeeded.
 */
bool CZMQToolkit::SendExistingMessage(zmsg_t *&message, void *socket)
{

   if (!socket || !message)
   {
      LOG(WARNING) << "Failed on send, NULL socket or message";
      if (message)
      {
         zmsg_destroy(&message);
      }
      return false;
   }
   bool success = true;
   if (zmsg_send(&message, socket) != 0)
   {

      int err = zmq_errno();
      std::string error(zmq_strerror(err));
      LOG(WARNING) << "Failed on send " << error;

      success = false;
   }
   if (message)
   {
      zmsg_destroy(&message);
   }
   return success;
}
