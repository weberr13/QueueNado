/* 
 * Author: Robert Weber
 *
 * Created on November 14, 2012, 2:48 PM
 */
#pragma once

#include <stdint.h>
#include <map>
#include <string>
#include <vector>

struct _zctx_t;
typedef struct _zctx_t zctx_t;
class Headcrab {
public:
   explicit Headcrab(const std::string& binding);
   virtual ~Headcrab();
   std::string GetBinding() const;
   void* GetContext() const;
   bool ComeToLife();

   void* GetFace(void* context);
   bool GetHitBlock(std::vector<std::string>& theHits);
   bool GetHitWait(std::vector<std::string>& theHit,const int timeout);
   bool SendSplatter(std::vector<std::string>& feedback);
   bool GetHitBlock(std::string& theHit);
   bool GetHitWait(std::string& theHit,const int timeout);
   bool SendSplatter(const std::string& feedback);
   static int GetHighWater();
private:

   void setIpcFilePermissions();
   Headcrab(const Headcrab& that) : mContext(NULL), mFace(NULL) {
   }

   std::string mBinding;
   void* mContext;
   void* mFace;
};

