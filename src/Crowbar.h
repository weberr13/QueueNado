/* 
 * Author: Robert Weber
 *
 * Created on November 14, 2012, 2:21 PM
 */
#pragma once
#include <stdint.h>

#include <map>
#include <string>
#include <set>
#include <vector>
#include "Headcrab.h"

struct _zctx_t;
typedef struct _zctx_t zctx_t;
class Crowbar {
public:
   explicit Crowbar(const std::string& binding);
   explicit Crowbar(const Headcrab& target);
   Crowbar(const std::string& binding, void* context);
   virtual ~Crowbar();

   bool Wield();
   bool Swing(const std::string& hit);
   bool Flurry( std::vector<std::string>& hits);
   bool BlockForKill(std::vector<std::string>& guts);
   bool WaitForKill(std::vector<std::string>& guts, const int timeout);
   bool BlockForKill(std::string& gut);
   bool WaitForKill(std::string& gut,const int timeout);
   void* GetTip();
   static int GetHighWater();
   void* GetContext();
private:
   bool PollForReady();
   Crowbar(const Crowbar& that) : mContext(NULL), mTip(NULL) {
   }

   void* mContext;
   std::string mBinding;
   void* mTip;
   bool mOwnsContext;
};
