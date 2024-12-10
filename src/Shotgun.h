#pragma once


#include <stdlib.h>
#include <vector>
#include <string>
struct _zctx_t;
typedef struct _zctx_t zctx_t;
class Shotgun {
public:
   Shotgun();
   void Aim(const std::string& location);
   void Fire(const std::string& msg);
   void Fire(const std::vector<std::string>& bullets);
   virtual ~Shotgun();
private:
   void setIpcFilePermissions(const std::string& location);
   void *mGun;
   void *mCtx;
};
