using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Morphology
{
    public class SentenceMorpher
    {
        private readonly StringBuilder _correctSentence = new();

        private static readonly char[] _sentenceSeparators = new char[2] { ' ', '\n' };
        private static readonly char[] _attributesSeparators = new char[2] { ' ', ',' };

        private const char OpeningAttributesBracket = '{';
        private const char ClosingAttributesBracket = '}';

        // Хранение по первому символу строки сделано для уменьшения
        // количества копируемых элементов при расширении словаря.
        // Была мысль ещё хранить не целиком слова, а только основную форму слова и окончания к нему,
        // но в итоге отказался от неё.
        public Dictionary<char, WordFormContainer> WordsByBeginningOfLine { get; } = new();

        /// <summary>
        ///     Создает <see cref="SentenceMorpher"/> из переданного набора строк словаря.
        /// </summary>
        /// <remarks>
        ///     В этом методе должен быть код инициализации: 
        ///     чтение и преобразование входных данных для дальнейшего их использования
        /// </remarks>
        /// <param name="dictionaryLines">
        ///     Строки исходного словаря OpenCorpora в формате plain-text.
        ///     <code> СЛОВО(знак_табуляции)ЧАСТЬ РЕЧИ( )атрибут1[, ]атрибут2[, ]атрибутN </code>
        /// </param>
        public static SentenceMorpher Create(IEnumerable<string> dictionaryLines)
        {
            var morpher = new SentenceMorpher();
            var isInitFormWord = true;
            var initFormWord = string.Empty;
            WordFormContainer? wordContainer = null;

            foreach (var line in dictionaryLines)
            {
                if (LineIsNotWord(line))
                {
                    isInitFormWord = true;
                    continue;
                }

                var wordForm = WordForm.Parse(line);

                if (isInitFormWord)
                {
                    var wordContainerKey = line[0];
                    wordContainer = morpher.GetWordContainer(wordContainerKey, out var isNewWordContainer);
                    wordContainer.Add(wordForm.Word, wordForm);

                    if (isNewWordContainer)
                    {
                        morpher.WordsByBeginningOfLine.Add(wordContainerKey, wordContainer);
                    }

                    initFormWord = wordForm.Word;
                    isInitFormWord = false;
                    continue;
                }

                AddMorphWordsTo(wordContainer!, wordForm, initFormWord);
            }

            return morpher;
        }

        private static bool LineIsNotWord(string line) => int.TryParse(line, out var _) || string.IsNullOrEmpty(line);

        private WordFormContainer GetWordContainer(char wordContainerKey, out bool isNewWordContainer)
        {
            isNewWordContainer = false;

            if (WordsByBeginningOfLine.TryGetValue(wordContainerKey, out var words))
            {
               return words;
            }

            isNewWordContainer = true;
            return new WordFormContainer();
        }

        private static void AddMorphWordsTo(WordFormContainer wordContainer, WordForm wordForm, string initFormWord)
        {
            var attributesByWord = wordContainer.GetWordForms(initFormWord);
            attributesByWord.Add(wordForm);
        }

        /// <summary>
        ///     Выполняет склонение предложения согласно указанному формату
        /// </summary>
        /// <param name="sentence">
        ///     Входное предложение <para/>
        ///     Формат: набор слов, разделенных пробелами.
        ///     После слова может следовать спецификатор требуемой части речи (формат описан далее),
        ///     если он отсутствует - слово требуется перенести в выходное предложение без изменений.
        ///     Спецификатор имеет следующий формат: <code>{ЧАСТЬ РЕЧИ,аттрибут1,аттрибут2,..,аттрибутN}</code>
        ///     Если для спецификации найдётся несколько совпадений - используется первое из них
        /// </param>
        public virtual string Morph(string sentence) // ApplyMorphology
        {
            if (string.IsNullOrWhiteSpace(sentence))
            {
                return string.Empty;
            }

            var toMorph = sentence.Split(_sentenceSeparators, StringSplitOptions.RemoveEmptyEntries);
        
            return GetCorrectSentence(toMorph);
        }

        private string GetCorrectSentence(string[] lines)
        {
            _correctSentence.Clear();

            var countSpace = lines.Length - 1;

            for (var i = 0; i < countSpace; i++)
            {
                _correctSentence.Append(GetCorrectFormOfWord(lines[i]) + " ");
            }

            _correctSentence.Append(GetCorrectFormOfWord(lines[countSpace]));

            return _correctSentence.ToString();
        }

        private string GetCorrectFormOfWord(string input)
        {
            var openBracketIndex = input.IndexOf(OpeningAttributesBracket);
            if(openBracketIndex == -1) 
            {
                return input;
            }

            var initWord = input[..openBracketIndex].ToUpperInvariant();
            if (input[openBracketIndex + 1] == ClosingAttributesBracket)
            {
                return initWord;
            }

            var words = WordsByBeginningOfLine[initWord[0]];
            if(!words.TryGetWordForms(initWord, out var wordForms))
            {
                return initWord;
            }

            var inputAttributes = input[(openBracketIndex + 1)..^1].Split(_attributesSeparators)
                                                                   .Select(x => x.ToUpperInvariant())
                                                                   .ToArray();

            var result = FindCorrectFormOfWord(wordForms!, inputAttributes);

            return string.IsNullOrEmpty(result) 
                 ? initWord 
                 : result;
        }

        private static string FindCorrectFormOfWord(IList<WordForm> wordForms, string[] inputAttributes)
        {
            foreach (var morphWord in wordForms)
            {
                var attributes = morphWord.Attributes;

                var matchCount = inputAttributes.Where(inputAtr => attributes.Contains(inputAtr))
                                                .Count();

                if (matchCount == inputAttributes.Length)
                {
                    return morphWord.Word;
                }
            }

            return string.Empty;
        }
    }

    public struct WordForm
    {
        private const char WordAttributesSeparator = '\t';
        private static readonly char[] _attributesSeparators = new char[2] { ' ', ',' };

        public string Word { get; }
        public HashSet<string> Attributes { get; }

        public WordForm(string word, IList<string> attributes)
        {
            Word = word;
            Attributes = new HashSet<string>(attributes);
        }

        public override string ToString() => $"{Word} {{{string.Join(',', Attributes.ToArray())}}}";

        public static WordForm Parse(string line)
        {
            var values = line.Split(WordAttributesSeparator);

            var word = values[0].ToUpperInvariant();

            var attributes = values[1].Split(_attributesSeparators, StringSplitOptions.RemoveEmptyEntries)
                                      .Select(x => x.ToUpperInvariant())
                                      .ToArray();

            return new WordForm(word, attributes);
        }
    }

    public class WordFormContainer
    {
        /// <summary>
        /// Слово в дефолтной форме - список словоформ с соответствующими атрибутами
        /// </summary>
        private readonly Dictionary<string, List<WordForm>> _formWordsByInitWord = new();

        public bool TryGetWordForms(string initWord, out List<WordForm>? wordForms) => _formWordsByInitWord.TryGetValue(initWord, out wordForms);

        public IList<WordForm> GetWordForms(string initWord) => _formWordsByInitWord[initWord];

        public void Add(string initWord, WordForm wordForm)
        {
            if (_formWordsByInitWord.TryGetValue(initWord, out var morphs))
            {
                morphs.Add(wordForm);
            }
            else
            {
                _formWordsByInitWord.Add(initWord, new List<WordForm>() { wordForm });
            }
        }
    }
}