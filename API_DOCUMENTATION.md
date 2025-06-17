# API Documentation - CGMF Models v1.1

## Base URL
```
Development: http://localhost:5000/api
Production: https://your-domain.com/api
```

## Authentication
Currently using API key-based authentication for external services. Future versions will implement JWT tokens for user authentication.

## Response Format
All API responses follow a consistent format:

```json
{
  "success": true,
  "data": {},
  "timestamp": "2025-06-17T08:00:00.000Z",
  "requestId": "uuid-v4"
}
```

## Error Responses
```json
{
  "success": false,
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid quartile value",
    "details": {}
  },
  "timestamp": "2025-06-17T08:00:00.000Z"
}
```

## Core APIs

### Fund Management

#### Get Top Rated Funds
```http
GET /api/funds/top-rated
```

**Response:**
```json
[
  {
    "fundId": 15,
    "fund": {
      "fundName": "Franklin India SHORT TERM INCOME PLAN - Direct - GROWTH",
      "category": "Debt",
      "subcategory": "Short Duration",
      "amcName": "Franklin Templeton Mutual Fund"
    },
    "totalScore": 90.5,
    "quartile": 1,
    "recommendation": "STRONG_BUY",
    "historicalReturnsTotal": 40,
    "riskGradeTotal": 30,
    "fundamentalsTotal": 10.5,
    "otherMetricsTotal": 10,
    "return1y": 16.3716
  }
]
```

#### Get Fund Details
```http
GET /api/funds/:id
```

**Parameters:**
- `id` (required): Fund ID

**Response:**
```json
{
  "id": 15,
  "schemeName": "Franklin India SHORT TERM INCOME PLAN",
  "category": "Debt",
  "subcategory": "Short Duration",
  "amcName": "Franklin Templeton Mutual Fund",
  "inceptionDate": "2003-01-01",
  "expenseRatio": 1.05,
  "exitLoad": 0.8,
  "benchmarkName": "Nifty 50 TRI",
  "minimumInvestment": 4000,
  "fundManager": "Fund Manager Name",
  "lockInPeriod": 4,
  "sector": "Debt",
  "elivateScore": {
    "totalScore": 90.5,
    "components": {
      "historicalReturns": 40,
      "riskGrade": 30,
      "fundamentals": 10.5,
      "otherMetrics": 10
    }
  }
}
```

### ELIVATE Scoring

#### Get Current ELIVATE Score
```http
GET /api/elivate/score
```

**Response:**
```json
{
  "score": 63,
  "interpretation": "NEUTRAL",
  "scoreDate": "2025-06-07T00:00:00.000Z",
  "components": {
    "marketSentiment": "NEUTRAL",
    "fundQuality": "GOOD",
    "riskLevel": "MODERATE"
  }
}
```

#### Get Fund ELIVATE Breakdown
```http
GET /api/elivate/breakdown/:fundId
```

**Parameters:**
- `fundId` (required): Fund ID

**Response:**
```json
{
  "fundId": 15,
  "totalScore": 90.5,
  "breakdown": {
    "historicalReturns": {
      "score": 40,
      "maxScore": 50,
      "components": {
        "return1y": 8.5,
        "return3y": 12.0,
        "return5y": 10.5,
        "ytd": 9.0
      }
    },
    "riskGrade": {
      "score": 30,
      "maxScore": 30,
      "components": {
        "volatility": 8,
        "sharpeRatio": 3.27,
        "maxDrawdown": 7,
        "beta": null
      }
    },
    "fundamentals": {
      "score": 10.5,
      "maxScore": 30,
      "components": {
        "expenseRatio": 4.5,
        "aumSize": 3.0,
        "ageMaturity": 3.0
      }
    },
    "otherMetrics": {
      "score": 10,
      "maxScore": 30,
      "components": {
        "momentum": 4,
        "consistency": 3,
        "forwardScore": 3
      }
    }
  }
}
```

### Portfolio Management

#### Get Available Portfolios
```http
GET /api/portfolios
```

**Response:**
```json
[
  {
    "id": 1,
    "name": "Conservative Portfolio",
    "riskProfile": "CONSERVATIVE",
    "targetReturn": 8.5,
    "maxVolatility": 12.0,
    "allocation": {
      "equity": 30,
      "debt": 60,
      "gold": 10
    },
    "funds": [
      {
        "fundId": 15,
        "allocation": 25,
        "weight": 0.25
      }
    ]
  }
]
```

#### Create Custom Portfolio
```http
POST /api/portfolios
```

**Request Body:**
```json
{
  "name": "Custom Growth Portfolio",
  "riskProfile": "MODERATE",
  "funds": [
    {
      "fundId": 15,
      "allocation": 30
    },
    {
      "fundId": 507,
      "allocation": 25
    }
  ],
  "rebalanceFrequency": "QUARTERLY"
}
```

### Backtesting Engine

#### Comprehensive Backtest
```http
POST /api/comprehensive-backtest
```

**Request Body:**
```json
{
  "portfolioId": 1,
  "riskProfile": "MODERATE",
  "quartile": "Q1",
  "elivateScoreRange": {
    "min": 80,
    "max": 90
  },
  "maxFunds": "5",
  "startDate": "2024-01-01",
  "endDate": "2024-12-31",
  "initialAmount": "100000",
  "rebalancePeriod": "quarterly",
  "scoreWeighting": true
}
```

**Response:**
```json
{
  "portfolioId": 0,
  "riskProfile": "Score-Based",
  "performance": {
    "totalReturn": 90.49,
    "annualizedReturn": 18.5,
    "monthlyReturns": [2.1, 3.4, -1.2],
    "bestMonth": 5.8,
    "worstMonth": -2.1,
    "positiveMonths": 9,
    "winRate": 75.0
  },
  "riskMetrics": {
    "volatility": 15.2,
    "maxDrawdown": -8.5,
    "sharpeRatio": 1.21,
    "sortinoRatio": 1.65,
    "calmarRatio": 2.18,
    "valueAtRisk95": -12.5,
    "betaToMarket": 0.85
  },
  "attribution": {
    "fundContributions": [
      {
        "fundId": 15,
        "fundName": "Franklin India SHORT TERM INCOME PLAN",
        "elivateScore": "90.5",
        "allocation": 33.33,
        "absoluteReturn": 16.37,
        "contribution": 5.46,
        "alpha": 2.3
      }
    ],
    "sectorContributions": [
      {
        "sector": "Debt",
        "allocation": 60,
        "contribution": 54.3
      }
    ]
  },
  "benchmark": {
    "benchmarkReturn": 12.5,
    "alpha": 6.0,
    "beta": 0.85,
    "trackingError": 4.2,
    "informationRatio": 1.43,
    "upCapture": 95.2,
    "downCapture": 78.5
  },
  "historicalData": [
    {
      "date": "2024-01-31",
      "portfolioValue": 102100,
      "benchmarkValue": 101050,
      "drawdown": 0
    }
  ]
}
```

### Quartile Analysis

#### Get Quartile Distribution
```http
GET /api/quartile/distribution
```

**Response:**
```json
{
  "totalCount": 11800,
  "q1Count": 2347,
  "q2Count": 2946,
  "q3Count": 3254,
  "q4Count": 3253,
  "distribution": {
    "Q1": 19.9,
    "Q2": 25.0,
    "Q3": 27.6,
    "Q4": 27.6
  }
}
```

#### Get Funds by Quartile
```http
GET /api/quartile/funds/:quartile
```

**Parameters:**
- `quartile` (required): 1, 2, 3, or 4

**Query Parameters:**
- `limit` (optional): Number of funds to return (default: 50, max: 1000)
- `offset` (optional): Pagination offset (default: 0)
- `category` (optional): Filter by fund category

**Response:**
```json
{
  "funds": [
    {
      "id": 15,
      "fundName": "Franklin India SHORT TERM INCOME PLAN",
      "category": "Debt",
      "subcategory": "Short Duration",
      "amcName": "Franklin Templeton Mutual Fund",
      "totalScore": 90.5,
      "quartile": 1,
      "recommendation": "STRONG_BUY",
      "return1y": 16.3716,
      "sector": "Debt"
    }
  ],
  "pagination": {
    "total": 2347,
    "limit": 50,
    "offset": 0,
    "hasMore": true
  }
}
```

#### Get Quartile Performance Metrics
```http
GET /api/quartile/metrics
```

**Response:**
```json
{
  "returnsData": [
    {
      "name": "Q1",
      "avgScore": 85.2,
      "avgReturn": 15.8,
      "fundCount": 2347
    },
    {
      "name": "Q2", 
      "avgScore": 72.4,
      "avgReturn": 12.1,
      "fundCount": 2946
    }
  ],
  "riskData": [
    {
      "quartile": "Q1",
      "avgVolatility": 12.5,
      "avgSharpe": 1.45,
      "avgMaxDrawdown": -8.2
    }
  ]
}
```

### Market Data

#### Get Market Indices
```http
GET /api/market/indices
```

**Response:**
```json
[
  {
    "indexName": "NIFTY 50",
    "indexDate": "2025-06-17",
    "indexValue": 22150.45,
    "dailyReturn": 0.85,
    "description": "Nifty 50 Index"
  },
  {
    "indexName": "NIFTY MIDCAP 100",
    "indexDate": "2025-06-17", 
    "indexValue": 52380.20,
    "dailyReturn": 1.12,
    "description": "Nifty Midcap 100 Index"
  }
]
```

#### Get Specific Index Data
```http
GET /api/market/index/:indexName
```

**Parameters:**
- `indexName` (required): URL-encoded index name

**Query Parameters:**
- `days` (optional): Number of days of historical data (default: 30)

**Response:**
```json
[
  {
    "indexName": "NIFTY 50",
    "indexDate": "2025-06-17",
    "indexValue": 22150.45,
    "dailyReturn": 0.85
  }
]
```

### Data Quality & ETL

#### Get ETL Status
```http
GET /api/etl/status
```

**Response:**
```json
[
  {
    "id": 2218,
    "pipelineName": "Fund Details Collection",
    "status": "COMPLETED",
    "startTime": "2025-06-17T08:36:15.833Z",
    "endTime": "2025-06-17T08:36:17.015Z",
    "recordsProcessed": 10,
    "errorMessage": null,
    "createdAt": "2025-06-17T08:36:15.869Z"
  }
]
```

#### Get Fund Details Collection Status
```http
GET /api/fund-details/status
```

**Response:**
```json
{
  "success": true,
  "status": {
    "totalFunds": 16766,
    "enhanced": 16766,
    "pending": 0,
    "percent": 100,
    "inProgress": false,
    "etlStatus": {
      "id": 2218,
      "pipelineName": "Fund Details Collection",
      "status": "COMPLETED",
      "startTime": "2025-06-17T08:36:15.833Z",
      "endTime": "2025-06-17T08:36:17.015Z",
      "recordsProcessed": 10,
      "errorMessage": "Successfully collected enhanced details for 10 out of 10 funds"
    }
  }
}
```

## Error Codes

| Code | Description |
|------|-------------|
| `VALIDATION_ERROR` | Invalid input parameters |
| `NOT_FOUND` | Resource not found |
| `DATA_INTEGRITY_ERROR` | Data quality issues detected |
| `EXTERNAL_API_ERROR` | External service unavailable |
| `CALCULATION_ERROR` | Mathematical calculation failed |
| `RATE_LIMIT_EXCEEDED` | API rate limit exceeded |
| `INSUFFICIENT_DATA` | Not enough data for calculation |

## Rate Limits

- **General APIs**: 1000 requests per hour per IP
- **Backtesting APIs**: 100 requests per hour per IP
- **Data Import APIs**: 50 requests per hour per IP

## Pagination

APIs that return lists support pagination:
- `limit`: Maximum number of items (default: 50, max: 1000)
- `offset`: Number of items to skip (default: 0)

## Data Freshness

All data includes freshness indicators:
- `lastUpdated`: When the data was last refreshed
- `dataAge`: Age of the data in milliseconds
- `staleThreshold`: When data is considered stale

## WebSocket Events (Future)

Real-time updates will be available via WebSocket:
- `fund-score-update`: ELIVATE score changes
- `market-data-update`: Real-time market data
- `portfolio-rebalance`: Portfolio rebalancing events