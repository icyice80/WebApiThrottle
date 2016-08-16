using System;
using System.Linq;
using Newtonsoft.Json;
using StackExchange.Redis;

namespace WebApiThrottle.Repositories
{
    /// <summary>
    /// Redis Implementation for IThrottleRepository
    /// e.g.
    /// var repo = new RedisCacheRepository(ConnectionMultiplexer.Connect("127.0.0.1:6379").GetDatabase(),"blah"), 
    /// </summary>
    public class RedisCacheRepository : IThrottleRepository
    {
        #region private fields
        private const string TotalRequest = "totalrequest";
        private const string TimeStamp = "timestamp";
        private readonly string _throttleKey;
        private readonly IDatabase _db;
        #endregion


        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="db">db instance</param>
        /// <param name="keyPrefix">use as prefix to construct the key in the redis</param>
        public RedisCacheRepository(IDatabase db, string keyPrefix)
        {
            _db = db;

            this._throttleKey = string.Concat(keyPrefix, ".AntiHammering.");
        }

       
        /// <summary>
        /// check the key's existance
        /// </summary>
        /// <param name="id">key</param>
        /// <returns>exists true else false</returns>
        public bool Any(string id)
        {
            return this._db.KeyExists(this.CreateKey(id));
        }

        /// <summary>
        /// no need to clear any key as it will expire based on its expiration time. 
        /// </summary>
        public void Clear()
        {
        }

        /// <summary>
        /// Find the throttle counter based on the given key
        /// </summary>
        /// <param name="id">Key</param>
        /// <returns>exists return the throttle counter, else returns null</returns>
        public ThrottleCounter? FirstOrDefault(string id)
        {

            var values = _db.HashGetAll(this.CreateKey(id));

            if (values.Any() && values.Count() == 2)
            {
                return new ThrottleCounter
                {
                    Timestamp = JsonConvert.DeserializeObject<DateTime>(values[1].Value),
                    TotalRequests = long.Parse(values[0].Value)
                };
            }

            return null;
        }

        /// <summary>
        /// Remove the key/value based on the given key
        /// </summary>
        /// <param name="id">Key</param>
        public void Remove(string id)
        {
            this._db.KeyDelete(this.CreateKey(id));
        }

        /// <summary>
        /// Save the throttle counter
        /// </summary>
        /// <param name="id">Key</param>
        /// <param name="throttleCounter">Throttle counter</param>
        /// <param name="expirationTime">Time to live</param>
        public void Save(string id, ThrottleCounter throttleCounter, TimeSpan expirationTime)
        {
            var key = this.CreateKey(id);
            if (throttleCounter.TotalRequests > 1)
            {
                this._db.HashIncrement(key, TotalRequest, 1);
            }
            else //its a new one.
            {
                var fieldValues = new HashEntry[]
                {
                    new HashEntry(TotalRequest, throttleCounter.TotalRequests),
                    new HashEntry(TimeStamp, JsonConvert.SerializeObject(throttleCounter.Timestamp))
                };

                var tran = this._db.CreateTransaction();
                tran.AddCondition(Condition.KeyNotExists(key));
                tran.HashSetAsync(key, fieldValues);
                tran.KeyExpireAsync(key, expirationTime);
                tran.Execute();
            }
        }

        /// <summary>
        /// combine the throttle key and key to construct the unique key
        /// </summary>
        /// <param name="key">key</param>
        /// <returns>key name in the store</returns>
        private string CreateKey(string key)
        {
            return this._throttleKey + key;
        }

    }
}
