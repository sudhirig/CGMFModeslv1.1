import express, { type Request, Response, NextFunction } from "express";
import { registerRoutes } from "./routes";
import { setupVite, serveStatic, log } from "./vite";

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.use((req, res, next) => {
  const start = Date.now();
  const path = req.path;
  let capturedJsonResponse: Record<string, any> | undefined = undefined;

  const originalResJson = res.json;
  res.json = function (bodyJson, ...args) {
    capturedJsonResponse = bodyJson;
    return originalResJson.apply(res, [bodyJson, ...args]);
  };

  res.on("finish", () => {
    const duration = Date.now() - start;
    if (path.startsWith("/api")) {
      let logLine = `${req.method} ${path} ${res.statusCode} in ${duration}ms`;
      if (capturedJsonResponse) {
        logLine += ` :: ${JSON.stringify(capturedJsonResponse)}`;
      }

      if (logLine.length > 80) {
        logLine = logLine.slice(0, 79) + "…";
      }

      log(logLine);
    }
  });

  next();
});

// Add quartile API routes BEFORE Vite middleware to prevent conflicts
app.get("/api/quartile/distribution", async (req, res) => {
  console.log("✓ DIRECT QUARTILE DISTRIBUTION ROUTE HIT!");
  const { storage } = await import("./storage");
  try {
    const category = req.query.category as string || undefined;
    const distribution = await storage.getQuartileDistribution(category);
    res.setHeader('Content-Type', 'application/json');
    res.json(distribution);
  } catch (error) {
    console.error("Error in direct quartile distribution:", error);
    res.status(500).json({ error: "Failed to fetch quartile distribution" });
  }
});

app.get("/api/quartile/metrics", async (req, res) => {
  console.log("✓ DIRECT QUARTILE METRICS ROUTE HIT!");
  const { storage } = await import("./storage");
  try {
    const metrics = await storage.getQuartileMetrics();
    res.setHeader('Content-Type', 'application/json');
    res.json(metrics);
  } catch (error) {
    console.error("Error in direct quartile metrics:", error);
    res.status(500).json({ error: "Failed to fetch quartile metrics" });
  }
});

app.get("/api/quartile/funds/:quartile", async (req, res) => {
  console.log("✓ DIRECT QUARTILE FUNDS ROUTE HIT!");
  const { storage } = await import("./storage");
  try {
    const quartile = parseInt(req.params.quartile);
    const category = req.query.category as string || undefined;
    
    if (isNaN(quartile) || quartile < 1 || quartile > 4) {
      return res.status(400).json({ error: "Invalid quartile. Must be a number between 1 and 4." });
    }
    
    const funds = await storage.getFundsByQuartile(quartile, category);
    res.setHeader('Content-Type', 'application/json');
    res.json(funds);
  } catch (error) {
    console.error("Error in direct quartile funds:", error);
    res.status(500).json({ error: "Failed to fetch funds by quartile" });
  }
});

(async () => {
  const server = await registerRoutes(app);

  app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
    const status = err.status || err.statusCode || 500;
    const message = err.message || "Internal Server Error";

    res.status(status).json({ message });
    throw err;
  });

  // importantly only setup vite in development and after
  // setting up all the other routes so the catch-all route
  // doesn't interfere with the other routes
  if (app.get("env") === "development") {
    await setupVite(app, server);
  } else {
    serveStatic(app);
  }

  // ALWAYS serve the app on port 5000
  // this serves both the API and the client.
  // It is the only port that is not firewalled.
  const port = 5000;
  server.listen({
    port,
    host: "0.0.0.0",
    reusePort: true,
  }, () => {
    log(`serving on port ${port}`);
  });
})();
