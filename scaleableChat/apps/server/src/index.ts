import "dotenv/config";
import http = require("node:http");
import SocketService = require("./services/socket");
import {startMessageConsumer} from './services/kafka';

async function init() {
  startMessageConsumer();
  const socketService = new SocketService();
  const httpServer = http.createServer();
  const PORT = Number(process.env.PORT) || 8000;

  await socketService.initListeners();
  socketService.io.attach(httpServer);

  httpServer.listen(PORT, () => {
    console.log(`HTTP Server started at PORT:${PORT}`);
  });
}

void init().catch((error: unknown) => {
  console.error("Failed to start Socket server:", error);
  process.exitCode = 1;
});
