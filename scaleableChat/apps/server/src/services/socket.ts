import SocketIO = require("socket.io");
import Redis = require("ioredis");
import prismaClient from "./prisma";
import { produceMessage } from "./kafka";

const redisUrl = process.env.REDIS_URL;

class SocketService {
  private readonly _io: SocketIO.Server;
  private readonly pub: Redis.Redis | null;
  private readonly sub: Redis.Redis | null;

  constructor() {
    console.log("Initializing Socket Service...");
    this._io = new SocketIO.Server({ cors: { origin: "*" } });

    if (redisUrl) {
      const pub = new Redis.default(redisUrl);
      const sub = new Redis.default(redisUrl);
      this.pub = pub;
      this.sub = sub;
      pub.on("error", (error: Error) => {
        console.error("Redis publisher error:", error.message);
      });
      sub.on("error", (error: Error) => {
        console.error("Redis subscriber error:", error.message);
      });
    } else {
      this.pub = null;
      this.sub = null;
      console.warn(
        "REDIS_URL is not set; using process-local message broadcasting.",
      );
    }
  }

  public async initListeners(): Promise<void> {
    const io = this.io;
    console.log("Initializing Socket Listeners...");

    if (this.pub && this.sub) {
      this.sub.on("message", (channel: string, message: string) => {
        if (channel === "MESSAGES") {
          io.emit("message", message);
        }
      });

      await Promise.all([this.pub.ping(), this.sub.subscribe("MESSAGES")]);
      console.log("Redis Pub/Sub connected.");
    }

    io.on("connection", (socket: SocketIO.Socket) => {
      console.log("New Socket Connected", socket.id);

      socket.on("event:message", async (payload: unknown) => {
        if (
          typeof payload !== "object" ||
          payload === null ||
          !("message" in payload) ||
          typeof payload.message !== "string"
        ) {
          console.warn("Ignoring invalid socket message payload.");
          return;
        }

        console.log("New Message Received", payload.message);

        try {
          await prismaClient.message.create({
            data: {
              text: payload.message,
            },
          });
        } catch (error: unknown) {
          console.error("Failed to store socket message:", error);
          socket.emit("message:error", "Message could not be stored.");
          return;
        }

        try {
          await produceMessage(payload.message);
        } catch (error: unknown) {
          console.error("Failed to produce socket message to Kafka:", error);
          socket.emit(
            "message:error",
            "Message was stored but could not be produced to Kafka.",
          );
          return;
        }

        if (this.pub) {
          try {
            await this.pub.publish("MESSAGES", payload.message);
          } catch (error: unknown) {
            console.error("Failed to publish socket message to Redis:", error);
            socket.emit(
              "message:error",
              "Message was stored and produced to Kafka but not broadcast.",
            );
          }
        } else {
          io.emit("message", payload.message);
        }
      });
    });
  }

  get io(): SocketIO.Server {
    return this._io;
  }
}

export = SocketService;
