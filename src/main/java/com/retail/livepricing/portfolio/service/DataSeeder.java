package com.retail.livepricing.portfolio.service;

import com.retail.livepricing.portfolio.entity.PortfolioEntity;
import com.retail.livepricing.portfolio.entity.PositionEntity;
import com.retail.livepricing.portfolio.entity.UserEntity;
import com.retail.livepricing.portfolio.entity.WatchlistEntity;
import com.retail.livepricing.portfolio.repo.PortfolioRepository;
import com.retail.livepricing.portfolio.repo.PositionRepository;
import com.retail.livepricing.portfolio.repo.UserRepository;
import com.retail.livepricing.portfolio.repo.WatchlistRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@Component
@ConditionalOnProperty(prefix = "app.seed", name = "enabled", havingValue = "true", matchIfMissing = true)
public class DataSeeder implements CommandLineRunner {
    private final UserRepository userRepository;
    private final PortfolioRepository portfolioRepository;
    private final PositionRepository positionRepository;
    private final WatchlistRepository watchlistRepository;
    private final StringRedisTemplate redisTemplate;

    public DataSeeder(UserRepository userRepository,
                      PortfolioRepository portfolioRepository,
                      PositionRepository positionRepository,
                      WatchlistRepository watchlistRepository,
                      StringRedisTemplate redisTemplate) {
        this.userRepository = userRepository;
        this.portfolioRepository = portfolioRepository;
        this.positionRepository = positionRepository;
        this.watchlistRepository = watchlistRepository;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void run(String... args) {
        if (userRepository.existsById("demo-user-1")) {
            return;
        }

        UserEntity user = new UserEntity();
        user.setId("demo-user-1");
        user.setEmail("demo-user-1@example.com");
        user.setTier("STANDARD");
        userRepository.save(user);

        PortfolioEntity portfolio = new PortfolioEntity();
        portfolio.setId(UUID.randomUUID().toString());
        portfolio.setUserId(user.getId());
        portfolioRepository.save(portfolio);

        seedPosition(portfolio.getId(), "AAPL", "15", "170", "2550");
        seedPosition(portfolio.getId(), "MSFT", "7", "390", "2730");
        seedPosition(portfolio.getId(), "NVDA", "3", "850", "2550");
        seedPosition(portfolio.getId(), "BTCUSD", "0.10", "68000", "6800");

        seedWatchlist(user.getId(), List.of("AMD", "GOOGL", "ETHUSD"));

        indexHolder(user.getId(), List.of("AAPL", "MSFT", "NVDA", "BTCUSD"));
    }

    private void seedPosition(String portfolioId, String symbol, String quantity, String avgCost, String costBasis) {
        PositionEntity p = new PositionEntity();
        p.setPortfolioId(portfolioId);
        p.setSymbol(symbol);
        p.setQuantity(new BigDecimal(quantity));
        p.setAvgCost(new BigDecimal(avgCost));
        p.setCostBasis(new BigDecimal(costBasis));
        positionRepository.save(p);
    }

    private void seedWatchlist(String userId, List<String> symbols) {
        for (String symbol : symbols) {
            WatchlistEntity w = new WatchlistEntity();
            w.setUserId(userId);
            w.setSymbol(symbol);
            watchlistRepository.save(w);
        }
    }

    private void indexHolder(String userId, List<String> symbols) {
        for (String symbol : symbols) {
            redisTemplate.opsForSet().add("holders:" + symbol, userId);
        }
    }
}
