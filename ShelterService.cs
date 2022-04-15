using Microservices.Common.Exceptions;
using Microservices.ExternalServices.Authorization;
using Microservices.ExternalServices.Billing;
using Microservices.ExternalServices.Billing.Types;
using Microservices.ExternalServices.CatDb;
using Microservices.ExternalServices.CatDb.Types;
using Microservices.ExternalServices.CatExchange;
using Microservices.ExternalServices.CatExchange.Types;
using Microservices.ExternalServices.Database;
using Microservices.Types;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Microservices
{
    public class CatShelterService : ICatShelterService
    {
        private readonly ConcurrentDictionary<string, Guid> _userIdBySessionId = new();

        private readonly ServiceExecutor _serviceExecutor = new();

        private readonly IDatabase _database;

        private readonly IAuthorizationService _authorizationService;
        private readonly IBillingService _billingService;
        private readonly ICatInfoService _catInfoService;
        private readonly ICatExchangeService _catExchangeService;

        public const int DefaultCatPrice = 1000;

        public CatShelterService(IDatabase database,
                                 IAuthorizationService authorizationService,
                                 IBillingService billingService,
                                 ICatInfoService catInfoService,
                                 ICatExchangeService catExchangeService)
        {
            _database = database;
            _authorizationService = authorizationService;
            _billingService = billingService;
            _catInfoService = catInfoService;
            _catExchangeService = catExchangeService;
        }

        public async Task<List<Cat>> GetCatsAsync(string sessionId, int skip, int limit, CancellationToken cancellationToken)
        {
            _ = await AuthorizeUser(sessionId, cancellationToken);

            var products = await _serviceExecutor.ExecuteAsync(_billingService.GetProductsAsync, skip, limit, cancellationToken);
            if (!products.Any())
            {
                return Enumerable.Empty<Cat>().ToList();
            }

            var catDbDocuments = await GetCatsFromDbAsync(products, cancellationToken);

            var catTasks = catDbDocuments
                .Select(async catDb =>
                {
                    var breedInfo = await _serviceExecutor.ExecuteAsync(_catInfoService.FindByBreedIdAsync,
                                                                        catDb.BreedId,
                                                                        cancellationToken);

                    return await CatFactory.CreateAsync(catDb, breedInfo);
                });

            var cats = await Task.WhenAll(catTasks);

            return cats.ToList();
        }

        private async Task<Guid> AuthorizeUser(string sessionId, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_userIdBySessionId.TryGetValue(sessionId, out var userId))
            {
                return userId;
            }

            return await _serviceExecutor.ExecuteAuthorizationAsync(_authorizationService,
                                                                    sessionId,
                                                                    cancellationToken,
                                                                    _userIdBySessionId);
        }

        private async Task<List<CatDbDocument>> GetCatsFromDbAsync(List<Product> products, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var catsDb = _database.GetCollection<CatDbDocument, Guid>(DbCollectionName.AllCats.GetStringValue());

            var comparer = new ProductComparer();
            var catsDbDocuments = await catsDb.FindAsync(catDb => products.Contains(new Product() { Id = catDb.Id }, comparer),
                                                        cancellationToken);

            return catsDbDocuments;
        }

        public async Task AddCatToFavouritesAsync(string sessionId, Guid catId, CancellationToken cancellationToken)
        {
            var userId = await AuthorizeUser(sessionId, cancellationToken);

            var product = await _serviceExecutor.ExecuteAsync(_billingService.GetProductAsync, catId, cancellationToken);
            if (product is null)
            {
                throw new InternalErrorException();
            }

            cancellationToken.ThrowIfCancellationRequested();
            var favouriteCatsDb = _database.GetCollection<FavouriteCats, Guid>(DbCollectionName.FavouriteCats.GetStringValue());

            var favouriteCats = await favouriteCatsDb.FindAsync(userId, cancellationToken);
            AddCatToFavourites(ref favouriteCats, catId, userId);

            await favouriteCatsDb.WriteAsync(favouriteCats, cancellationToken);
        }

        private void AddCatToFavourites(ref FavouriteCats favouriteCats, Guid catId, Guid userId)
        {
            // ref заюзал, т.к. не совсем очевидно, что метод Add, который обычно
            // либо не возвращает ничего, либо bool, должен вернуть ссылку на объект.
            // Применение ref явным образом подчеркивает, что я хочу работать с оригиналом ссылки.

            if (favouriteCats is null)
            {
                favouriteCats = new FavouriteCats()
                {
                    Id = userId,
                    CatsId = new HashSet<Guid>()
                };
            }

            favouriteCats.CatsId.Add(catId);
        }

        public async Task<List<Cat>> GetFavouriteCatsAsync(string sessionId, CancellationToken cancellationToken)
        {
            var userId = await AuthorizeUser(sessionId, cancellationToken);

            cancellationToken.ThrowIfCancellationRequested();
            var favouriteCatsDb = _database.GetCollection<FavouriteCats, Guid>(DbCollectionName.FavouriteCats.GetStringValue());

            var favouriteCats = await favouriteCatsDb.FindAsync(userId, cancellationToken);
            if (favouriteCats is null)
            {
                return Enumerable.Empty<Cat>().ToList();
            }

            cancellationToken.ThrowIfCancellationRequested();
            var catsDb = _database.GetCollection<CatDbDocument, Guid>(DbCollectionName.AllCats.GetStringValue());

            return await GetFavouriteCatsAsync(favouriteCats, catsDb, cancellationToken);
        }

        private async Task<List<Cat>> GetFavouriteCatsAsync(FavouriteCats favouriteCats,
                                                            IDatabaseCollection<CatDbDocument, Guid> catsDb,
                                                            CancellationToken cancellationToken)
        {
            var result = new List<Cat>();
            var catDbDocuments = await catsDb.FindAsync(catDb => favouriteCats.CatsId.Contains(catDb.Id), cancellationToken);

            foreach (var catDb in catDbDocuments)
            {
                var product = await _serviceExecutor.ExecuteAsync(_billingService.GetProductAsync, catDb.Id, cancellationToken);
                if (product is null)
                {
                    continue;
                }

                var catInfo = await _serviceExecutor.ExecuteAsync(_catInfoService.FindByBreedIdAsync, product.BreedId, cancellationToken);

                result.Add(await CatFactory.CreateAsync(catDb, catInfo));
            }

            return result;
        }

        public async Task DeleteCatFromFavouritesAsync(string sessionId, Guid catId, CancellationToken cancellationToken)
        {
            var userId = await AuthorizeUser(sessionId, cancellationToken);

            cancellationToken.ThrowIfCancellationRequested();
            var favouritesCatsDb = _database.GetCollection<FavouriteCats, Guid>(DbCollectionName.FavouriteCats.GetStringValue());

            var favouriteCats = await favouritesCatsDb.FindAsync(userId, cancellationToken);
            if (favouriteCats is null)
            {
                return;
            }

            // Обновлять саму бд не надо. ¯\_(ツ)_/¯ 
            favouriteCats.CatsId.Remove(catId);
        }

        public async Task<Bill> BuyCatAsync(string sessionId, Guid catId, CancellationToken cancellationToken)
        {
            _ = await AuthorizeUser(sessionId, cancellationToken);

            var product = await _serviceExecutor.ExecuteAsync(_billingService.GetProductAsync, catId, cancellationToken);
            if (product is null)
            {
                throw new InvalidRequestException();
            }

            var catPriceHistory = await _serviceExecutor.ExecuteAsync(_catExchangeService.GetPriceInfoAsync, product.BreedId, cancellationToken);

            var price = catPriceHistory.Prices.Any()
                      ? catPriceHistory.Prices[^1].Price
                      : DefaultCatPrice;

            return await _serviceExecutor.ExecuteAsync(_billingService.SellProductAsync, product.Id, price, cancellationToken);
        }

        public async Task<Guid> AddCatAsync(string sessionId, AddCatRequest request, CancellationToken cancellationToken)
        {
            var userId = await AuthorizeUser(sessionId, cancellationToken);

            var catInfo = await _serviceExecutor.ExecuteAsync(_catInfoService.FindByBreedNameAsync, request.Breed, cancellationToken);
            var catPrice = await _serviceExecutor.ExecuteAsync(_catExchangeService.GetPriceInfoAsync, catInfo.BreedId, cancellationToken);

            // Вообще говоря, собирать кота необязательно, все данные и так есть, но кота проще по методам таскать.
            var cat = await CatFactory.CreateAsync(request, catInfo, catPrice, userId);

            await AddCatToDbAsync(cat, cancellationToken);
            await AddCatToBillingServiceAsync(cat, cancellationToken);

            return cat.Id;
        }

        private async Task AddCatToDbAsync(Cat cat, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var catsDb = _database.GetCollection<CatDbDocument, Guid>(DbCollectionName.AllCats.GetStringValue());

            await catsDb.WriteAsync(new CatDbDocument
            {
                AddedBy = cat.AddedBy,
                CatPhoto = cat.CatPhoto,
                Id = cat.Id,
                Name = cat.Name,
                Price = cat.Price,
                Prices = cat.Prices,
                BreedId = cat.BreedId
            }, cancellationToken);
        }

        private async Task AddCatToBillingServiceAsync(Cat cat, CancellationToken cancellationToken)
        {
            var product = new Product() { Id = cat.Id, BreedId = cat.BreedId };
            await _serviceExecutor.ExecuteAsync(_billingService.AddProductAsync, product, cancellationToken);
        }
    }

    public class CatFactory
    {
        public static Task<Cat> CreateAsync(CatDbDocument catDb, CatInfo catInfo)
        {
            return Task.Run(() => new Cat
            {
                Breed = catInfo.BreedName,
                BreedId = catInfo.BreedId,
                BreedPhoto = catInfo.Photo,
                Id = catDb.Id,
                Name = catDb.Name,
                AddedBy = catDb.AddedBy,
                Prices = catDb.Prices,
                Price = catDb.Price,
                CatPhoto = catDb.CatPhoto
            });
        }

        public static Task<Cat> CreateAsync(AddCatRequest request, CatInfo catInfo, CatPriceHistory catPrice, Guid addedBy)
        {
            var pricesIsNotEmpty = catPrice.Prices.Any();

            var price = pricesIsNotEmpty
                      ? catPrice.Prices[^1].Price
                      : CatShelterService.DefaultCatPrice;

            var prices = pricesIsNotEmpty
                       ? catPrice.Prices.Select(x => (x.Date, x.Price)).ToList()
                       : new List<(DateTime Date, decimal Price)>();

            return Task.Run(() => new Cat
            {
                Name = request.Name,
                CatPhoto = request.Photo,
                BreedId = catInfo.BreedId,
                Breed = request.Breed,
                AddedBy = addedBy,
                BreedPhoto = catInfo.Photo,
                Id = Guid.NewGuid(),
                Price = price,
                Prices = prices
            });
        }
    }

    public class ServiceExecutor
    {
        public int AttemptCounterWithConnectionException { get; }

        public ServiceExecutor() => AttemptCounterWithConnectionException = 2;
        public ServiceExecutor(int attemptCountConnection) => AttemptCounterWithConnectionException = attemptCountConnection;

        public async Task<TResult> ExecuteAsync<TParam, TResult>(Func<TParam, CancellationToken, Task<TResult>> get,
                                                                 TParam param,
                                                                 CancellationToken cancellationToken)
        {
            TResult result = default;
            var counter = AttemptCounterWithConnectionException;
            while (counter != 0)
            {
                try
                {
                    result = await get(param, cancellationToken);
                }
                catch (ConnectionException)
                {
                    counter--;
                    continue;
                }

                break;
            }

            if (counter == 0)
            {
                throw new InternalErrorException();
            }

            return result;
        }

        public async Task<TResult> ExecuteAsync<TParam1, TParam2, TResult>(Func<TParam1, TParam2, CancellationToken, Task<TResult>> get,
                                                                           TParam1 param1,
                                                                           TParam2 param2,
                                                                           CancellationToken cancellationToken)
        {
            TResult result = default;

            var counter = AttemptCounterWithConnectionException;
            while (counter != 0)
            {
                try
                {
                    result = await get(param1, param2, cancellationToken);
                }
                catch (ConnectionException)
                {
                    counter--;
                    continue;
                }

                break;
            }

            if (counter == 0)
            {
                throw new InternalErrorException();
            }

            return result;
        }

        public async Task ExecuteAsync<TParam>(Func<TParam, CancellationToken, Task> add,
                                               TParam param,
                                               CancellationToken cancellationToken)
        {
            var counter = AttemptCounterWithConnectionException;
            while (counter != 0)
            {
                try
                {
                    await add(param, cancellationToken);
                }
                catch (ConnectionException)
                {
                    counter--;
                    continue;
                }

                break;
            }

            if (counter == 0)
            {
                throw new InternalErrorException();
            }
        }

        public async Task<Guid> ExecuteAuthorizationAsync(IAuthorizationService authorizationService,
                                                          string sessionId,
                                                          CancellationToken cancellationToken,
                                                          ConcurrentDictionary<string, Guid> userIdBySessionId)
        {
            var counter = AttemptCounterWithConnectionException;
            while (counter != 0)
            {
                try
                {
                    var authResult = await authorizationService.AuthorizeAsync(sessionId, cancellationToken);
                    if (!authResult.IsSuccess)
                    {
                        throw new AuthorizationException();
                    }

                    userIdBySessionId.AddOrUpdate(sessionId, authResult.UserId, (session, id) => id);

                    return authResult.UserId;
                }
                catch (ConnectionException)
                {
                    counter--;
                    continue;
                }

                break;
            }

            throw new InternalErrorException();
        }
    }

    public class CatDbDocument : IEntityWithId<Guid>
    {
        public Guid Id { get; set; }

        public Guid AddedBy { get; set; }

        public Guid BreedId { get; set; }

        public string Name { get; set; }

        public byte[] CatPhoto { get; set; }

        public decimal Price { get; set; }

        public List<(DateTime Date, decimal Price)> Prices { get; set; }
    }

    public class FavouriteCats : IEntityWithId<Guid>
    {
        public Guid Id { get; set; }

        public HashSet<Guid> CatsId { get; set; }
    }

    public class StringValueAttribute : Attribute
    {
        public string StringValue { get; set; }

        public StringValueAttribute(string stringValue) => StringValue = stringValue;
    }

    public enum DbCollectionName
    {
        [StringValueAttribute(nameof(FavouriteCats))] FavouriteCats,
        [StringValueAttribute(nameof(AllCats))] AllCats,
    }

    public static class EnumExtensions
    {
        public static string GetStringValue(this Enum value)
        {
            var type = value.GetType();

            var fieldInfo = type.GetField(value.ToString());

            var attributes = fieldInfo.GetCustomAttributes(typeof(StringValueAttribute), false) as StringValueAttribute[];

            return attributes.Length > 0
                 ? attributes[0].StringValue
                 : null;
        }
    }

    public class ProductComparer : IEqualityComparer<Product>
    {
        public bool Equals(Product x, Product y)
        {
            if (object.ReferenceEquals(x, y))
            {
                return true;
            }

            if (x is null || y is null)
            {
                return false;
            }

            return x.Id == y.Id;
        }

        public int GetHashCode(Product obj) => obj.GetHashCode();
    }
}