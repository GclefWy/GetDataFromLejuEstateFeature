using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace sinaData
{
    class HttpHelper
    {
        public static string getHttp(string url, Dictionary<string, string> param)
        {
            string QueryStr = "";
            foreach (var item in param)
            {
                QueryStr += item.Key + "=" + item.Value + "&";
            }
            QueryStr = "?" + QueryStr.Remove(QueryStr.Length - 1, 1);

            return getHttp(url, QueryStr);
        }

        public static string getHttp(string url, string queryString)
        {
            HttpWebRequest httpWebRequest = (HttpWebRequest)WebRequest.Create(url + queryString);

            httpWebRequest.ContentType = "application/json";
            httpWebRequest.Method = "GET";
            httpWebRequest.Timeout = 20000;

            string responseContent = "";
            HttpWebResponse httpWebResponse;
            StreamReader streamReader;
            try
            {
                httpWebResponse = (HttpWebResponse)httpWebRequest.GetResponse();
                streamReader = new StreamReader(httpWebResponse.GetResponseStream());
                responseContent = streamReader.ReadToEnd();
                httpWebResponse.Close();
                streamReader.Close();
            }
            catch
            {
                responseContent = "";

            }
            finally
            {
            }

            return responseContent;
        }
    }
}
