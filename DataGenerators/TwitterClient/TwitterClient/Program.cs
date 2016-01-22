//********************************************************* 
// 
//    Copyright (c) Microsoft. All rights reserved. 
//    This code is licensed under the Microsoft Public License. 
//    THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF 
//    ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING ANY 
//    IMPLIED WARRANTIES OF FITNESS FOR A PARTICULAR 
//    PURPOSE, MERCHANTABILITY, OR NON-INFRINGEMENT. 
// 
//*********************************************************

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Threading;
using System.Diagnostics;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace TwitterClient
{
    class Program
    {
        static void Main(string[] args)
        {
            ReadConfig();

            //Configure Twitter OAuth
            var oauthToken = ConfigurationManager.AppSettings["oauth_token"];
            var oauthTokenSecret = ConfigurationManager.AppSettings["oauth_token_secret"];
            var oauthCustomerKey = ConfigurationManager.AppSettings["oauth_consumer_key"];
            var oauthConsumerSecret = ConfigurationManager.AppSettings["oauth_consumer_secret"];
            var keywords = ConfigurationManager.AppSettings["twitter_keywords"];

            //Configure EventHub
            var config = new EventHubConfig();
            config.ConnectionString = ConfigurationManager.AppSettings["EventHubConnectionString"];
            config.EventHubName = ConfigurationManager.AppSettings["EventHubName"];
            var myEventHubObserver = new EventHubObserver(config);

            var thread = new Thread(new ThreadStart(Program.DoHeartbeat));
            thread.Start();

            var datum = 
                Tweet.StreamStatuses(new TwitterConfig(oauthToken, oauthTokenSecret, oauthCustomerKey, oauthConsumerSecret, keywords))
                .Select(tweet => Sentiment.ComputeScore(tweet))
                .Select(tweet => new Payload { CreatedAt = tweet.CreatedAt, UtcOffset = tweet.UtcOffset, UserName = tweet.UserName, SentimentScore = tweet.SentimentScore });

            datum.ToObservable().Subscribe(myEventHubObserver);
           

        }

        public static void DoHeartbeat()
        {
            while (true)
            {
                Trace.TraceInformation("HEARTBEAT");
                Thread.Sleep(60000);
            }
        }

        public static void ReadConfig()
        {
            var secretsFile = Path.Combine(System.AppDomain.CurrentDomain.BaseDirectory, "secrets.json");
            if (File.Exists(secretsFile))
            {
                using (var reader = new StreamReader(secretsFile))
                {
                    using (var jsonReader = new JsonTextReader(reader))
                    {
                        var serializer = new JsonSerializer();
                        var json = serializer.Deserialize<JObject>(jsonReader);
                        foreach (var entry in json)
                        {
                            ConfigurationManager.AppSettings.Set(entry.Key, (string)entry.Value);
                        }
                    }
                }
            }
        }
    }
}
