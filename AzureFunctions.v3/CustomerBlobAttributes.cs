using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace FileValidation
{
    public class CustomerBlobAttributes
    {
        static readonly Regex blobUrlRegexExtract = new Regex(@"^\S*/([^/]+)/inbound/((([^_]+)_([\d]+_[\d]+))_([\w]+))\.csv$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

        static readonly Regex blobUrlRegexExtractWithSubfolder = new Regex(@"^\S*/([^/]+)/([^/]+)/((([^_]+)_([\d]+_[\d]+))_([\w]+))\.csv$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

        public string FullUrl { get; private set; }
        public string Filename { get; private set; }
        public string BatchPrefix { get; private set; }
        public DateTime BatchDateTime { get; private set; }
        public string Filetype { get; private set; }
        public string CustomerName { get; private set; }
        public string ContainerName { get; private set; }
        public string Subfolder { get; private set; }

        public static CustomerBlobAttributes Parse(string fullUri, bool detectSubfolder = false)
        {
            if (detectSubfolder)
            {
                var regexMatch = blobUrlRegexExtractWithSubfolder.Match(fullUri);
                if (regexMatch.Success)
                {
                    return new CustomerBlobAttributes
                    {
                        FullUrl = regexMatch.Groups[0].Value,
                        ContainerName = regexMatch.Groups[1].Value,
                        Subfolder = regexMatch.Groups[2].Value,
                        Filename = regexMatch.Groups[3].Value,
                        BatchPrefix = regexMatch.Groups[4].Value,
                        CustomerName = regexMatch.Groups[5].Value,
                        BatchDateTime = DateTime.ParseExact(regexMatch.Groups[6].Value, @"yyyyMMdd_HHmm", CultureInfo.InvariantCulture),
                        Filetype = regexMatch.Groups[7].Value
                    };
                }
            }
            else
            {
                var regexMatch = blobUrlRegexExtract.Match(fullUri);
                if (regexMatch.Success)
                {
                    return new CustomerBlobAttributes
                    {
                        FullUrl = regexMatch.Groups[0].Value,
                        ContainerName = regexMatch.Groups[1].Value,
                        Filename = regexMatch.Groups[2].Value,
                        BatchPrefix = regexMatch.Groups[3].Value,
                        CustomerName = regexMatch.Groups[4].Value,
                        BatchDateTime = DateTime.ParseExact(regexMatch.Groups[5].Value, @"yyyyMMdd_HHmm", CultureInfo.InvariantCulture),
                        Filetype = regexMatch.Groups[6].Value
                    };
                }
            }

            return null;
        }
    }
}
