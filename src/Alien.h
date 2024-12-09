#pragma once


#include <stdlib.h>
#include <vector>
#include <string>
struct _zctx_t;
typedef struct _zctx_t zctx_t;
class Alien {
public:
   Alien();
   void PrepareToBeShot(const std::string& location);
   std::vector<std::string> GetShot();
   void GetShot(const unsigned int timeout, std::vector<std::string>& bullets);
   virtual ~Alien();
    
private:
   void *mBody;
   void *mCtx;
};