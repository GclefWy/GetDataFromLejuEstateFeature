using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sinaData
{
    class SinaSignHelper
    {
        public static int ConvertDateTimeInt(System.DateTime time)
        {
            System.DateTime startTime = TimeZone.CurrentTimeZone.ToLocalTime(new System.DateTime(1970, 1, 1));
            return (int)(time - startTime).TotalSeconds;
        }

        private static string SHA1(string text)
        {
            byte[] cleanBytes = Encoding.Default.GetBytes(text);
            byte[] hashedBytes = System.Security.Cryptography.SHA1.Create().ComputeHash(cleanBytes);
            return BitConverter.ToString(hashedBytes).Replace("-", "");
        }

        public static string generateNonceStr(int length = 16)
        {
            char[] chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".ToCharArray();
            string str = "";

            Random ran = new Random();

            for (int i = 0; i < length; i++)
            {
                str += chars[ran.Next(0, chars.Length - 1)];
            }

            return str;
        }

        public static string getSignature(Dictionary<string, string> arrdata, string app_key)
        {
            string paramstring = "";

            var dicSortKey = from objDic in arrdata orderby objDic.Key select objDic;

            foreach (KeyValuePair<string, string> kvp in dicSortKey)
            {
                if (paramstring.Length > 0)
                {
                    paramstring += "&";
                }

                paramstring += kvp.Key + "=" + kvp.Value;
            }

            paramstring += app_key;

            var dicSortValue = from objDic in arrdata orderby objDic.Value select objDic;

            foreach (KeyValuePair<string, string> kvp in dicSortValue)
            {
                paramstring += kvp.Value;
            }

            return SHA1(paramstring);
        }
    }
}
