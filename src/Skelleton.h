#pragma once
#include <zmq.h>
#include <czmq.h>

class Skelleton {
public:

   explicit Skelleton(const std::string& binding) : mBinding(binding), mContext(NULL), mFace(NULL) {

   }

   virtual ~Skelleton() {
      if (mContext) {
         zmq_ctx_destroy(&mContext);
      }
   }

   virtual bool Initialize() {
      return false;
   }
protected:
   std::string mBinding;
   void* mContext;
   void* mFace;
};

