#pragma once
#include <string>
#include <map>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
struct _zctx_t;
typedef struct _zctx_t zctx_t;

class BoomStick {
public:
   explicit BoomStick(const std::string& binding);
   BoomStick(BoomStick&& other);
   virtual ~BoomStick();

   BoomStick& operator=(BoomStick&& other);
   virtual bool Initialize();
   virtual std::string Send(const std::string& command);
   virtual bool SendAsync(const std::string& uuid, const std::string& command);
   virtual bool GetAsyncReply(const std::string& uuid, const unsigned int msToWait, std::string& reply);
   std::string GetUuid();
   void Swap(BoomStick& other);
   void SetBinding(const std::string& binding);
   void SetSendHWM(const int hwm);
   void SetRecvHWM(const int hwm);
   zctx_t* GetContext();
protected:
   virtual zctx_t* GetNewContext();
   virtual void* GetNewSocket(zctx_t* ctx);
   virtual bool ConnectToBinding(void* socket, const std::string& binding);
   virtual bool FindPendingUuid(const std::string& uuid) const;
   bool FindUnreadUuid(const std::string& uuid) const;
   virtual void CleanOldPendingData();
   virtual void CleanPendingReplies();
   virtual void CleanUnreadReplies();
   virtual bool GetReplyFromSocket(const std::string& uuid, const unsigned int msToWait, std::string& reply);
   virtual bool GetReplyFromCache(const std::string& uuid, std::string& reply);
   virtual bool CheckForMessagePending(const std::string& messageHash, const unsigned int msToWait, std::string& reply);
   virtual bool ReadFromReadySocket(std::string& foundId, std::string& foundReply);

   std::map<std::string, std::string> mUnreadReplies;
   time_t mLastGCTime;
private:
   std::map<std::string, time_t> mPendingReplies;
   std::string mBinding;
   void *mChamber;
   zctx_t *mCtx;
   //boost::mt19937 mRan;
   boost::uuids::basic_random_generator<boost::mt19937> m_uuidGen;
   int mSendHWM;
   int mRecvHWM;
   unsigned int mPendingAlertSize;
   unsigned int mUnreadAlertSize;
   bool mUnreadAlert;
   bool mPendingAlert;
   pthread_t mUtilizedThread;
};
