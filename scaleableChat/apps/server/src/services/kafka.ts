import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { Kafka, type Producer } from "kafkajs";
import prismaClient from "./prisma";

const brokers = (process.env.KAFKA_BROKERS ??
  "kafka-3337a43e-dasmaitreya87-0744.j.aivencloud.com:23932")
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);

if (brokers.length === 0) {
  throw new Error("KAFKA_BROKERS must contain at least one broker.");
}

const caPath = path.resolve(
  process.env.KAFKA_CA_PATH ?? path.resolve(__dirname, "../../ca.pem"),
);
const kafkaUsername = process.env.KAFKA_USERNAME;
const kafkaPassword = process.env.KAFKA_PASSWORD;

if (
  brokers.some((broker) => broker.includes(".aivencloud.com")) &&
  (!kafkaUsername || !kafkaPassword)
) {
  throw new Error(
    "KAFKA_USERNAME and KAFKA_PASSWORD must be set when connecting to an Aiven Kafka broker.",
  );
}

if (Boolean(kafkaUsername) !== Boolean(kafkaPassword)) {
  throw new Error(
    "KAFKA_USERNAME and KAFKA_PASSWORD must either both be set or both be omitted.",
  );
}

const kafka = new Kafka({
  brokers,
  ssl: {
    ca: [fs.readFileSync(caPath, "utf8")],
  },
  ...(kafkaUsername && kafkaPassword
    ? {
        sasl: {
          username: kafkaUsername,
          password: kafkaPassword,
          mechanism: "plain" as const,
        },
      }
    : {}),
});

let producerPromise: Promise<Producer> | null = null;

export function createProducer(): Promise<Producer> {
  if (!producerPromise) {
    const producer = kafka.producer();
    producerPromise = producer.connect().then(
      () => producer,
      (error: unknown) => {
        producerPromise = null;
        throw error;
      },
    );
  }

  return producerPromise;
}

export async function produceMessage(message: string): Promise<void> {
  const producer = await createProducer();
  const topic = process.env.KAFKA_TOPIC || "MESSAGES";
  await producer.send({
    topic,
    messages: [{ key: randomUUID(), value: message }],
  });
  console.info("Message produced to Kafka topic:", topic);
}

export async function startMessageConsumer(){
    console.log('Consumer is running..');
    const consumer=await kafka.consumer({groupId: "default"});
    await consumer.connect();
    await consumer.subscribe({topic: "MESSAGES",fromBeginning:true});
    await consumer.run({
        autoCommit: true,
        eachMessage: async ({message,pause})=>{
            if(!message.value) return;
            console.log(`New Message Received...`)
            try{
            await prismaClient.message.create({
                data:{
                    text:message.value?.toString(),

                },
            });
        }catch(err){
            console.log('Something is wrong')
            pause();
            setTimeout(()=>{consumer.resume([{topic: 'MESSAGES'}])},60*1000)
        }
        } 
    })
}

export default kafka;
