using System;
using System.Collections.Generic;
using System.Linq;

namespace AutoComplete
{
    public class AutoCompleter
    {
        private readonly Dictionary<char, List<FullName>> _fullNames = new();

        public const int MaxPrefixLength = 100;

        public void AddToSearch(List<FullName> fullNames)
        {
            foreach (var fullName in fullNames.Select(fn => fn.Trim())
                                              .OrderBy(fn => fn))
            {
                var firstSymbol = fullName.GetFirstSymbol();

                if (_fullNames.TryGetValue(firstSymbol, out var names))
                {
                    names.Add(fullName);
                }
                else
                {
                    _fullNames.Add(firstSymbol, new List<FullName> { fullName });
                }
            }
        }

        public List<string> Search(string prefix)
        {
            ValidatePrefix(prefix);

            var fullNames = _fullNames[prefix[0]];
            var index = fullNames.BinarySearch(new FullName { Surname = prefix }, new PrefixFullNameComparer());
            if (index == -1)
            {
                return Enumerable.Empty<string>().ToList();
            }
           
            var minIndex = FindBorderIndex(prefix, index, index => --index);
            var maxIndex = FindBorderIndex(prefix, index, index => ++index);
            if(maxIndex == minIndex)
            {
                return new List<string> { fullNames[maxIndex].ToString() };
            }

            return fullNames.GetRange(minIndex, maxIndex - minIndex + 1)
                            .Select(x => x.ToString())
                            .ToList();
        }

        private static void ValidatePrefix(string prefix)
        {
            if (string.IsNullOrWhiteSpace(prefix))
            {
                throw new ArgumentException($"{nameof(prefix)} is empty.");
            }

            if (prefix.Length > MaxPrefixLength)
            {
                throw new ArgumentException($"Request length is too long. Was: {prefix.Length}. Max: {MaxPrefixLength}");
            }
        }

        private int FindBorderIndex(string prefix, int startIndex, Func<int, int> moveIndex)
        {
            var prefixLength = prefix.Length;
            var fullNames = _fullNames[prefix[0]];
            var prefixHash = prefix.GetHashCode();
            var result = startIndex;

            while (startIndex >= 0 && startIndex < fullNames.Count)
            {
                var fullName = fullNames[startIndex].ToString();
                var substring = fullName[..prefixLength];

                if (substring.GetHashCode() != prefixHash)
                {
                    break;
                }

                if (fullName.StartsWith(prefix, StringComparison.Ordinal))
                {
                    result = startIndex;
                }

                startIndex = moveIndex(startIndex);
            }

            return result;
        }
    }

    public struct FullName : IComparable<FullName>
    {
        public string? Name { get; set; }
        public string? Surname { get; set; }
        public string? Patronymic { get; set; }

        public FullName Trim()
        {
            return new FullName()
            {
                Name = Name?.Trim(),
                Surname = Surname?.Trim(),
                Patronymic = Patronymic?.Trim(),
            };
        }

        public char GetFirstSymbol()
        {
            var value = ToString();

            return string.IsNullOrEmpty(value) 
                 ? throw new ArgumentNullException(nameof(FullName), "All members is null.")
                 : value[0];
        }

        public int CompareTo(FullName other) => ToString().CompareTo(other.ToString());

        public override string ToString()
        {
            return $"{Surname}" +
                   $"{(RequiredWhiteSpace(Surname, Name) ? " " : string.Empty)}" +
                   $"{Name}" +
                   $"{(RequiredWhiteSpace(Name, Patronymic) ? " " : (RequiredWhiteSpace(Surname, Patronymic) ? " " : string.Empty))}" +
                   $"{Patronymic}";
        }

        private static bool RequiredWhiteSpace(string? value1, string? value2) => !string.IsNullOrEmpty(value1) && !string.IsNullOrEmpty(value2);
    }

    public class PrefixFullNameComparer : IComparer<FullName>
    {
        public int Compare(FullName fullName, FullName nameFromPrefix)
        {
            var fullNameStr = fullName.ToString();
            var prefix = nameFromPrefix.ToString();

            var subFullName = fullNameStr[..prefix.Length];

            return subFullName.CompareTo(prefix);
        }
    }
}