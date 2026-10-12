"use client";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { io, type Socket } from "socket.io-client";

interface SocketProviderProps {
  children: ReactNode;
}

interface ISocketContext {
  sendMessage: (message: string) => void;
  messages: string[];
}

const SocketContext = createContext<ISocketContext | null>(null);

export const useSocket = () => {
  const state = useContext(SocketContext);
  if (!state) {
    throw new Error("useSocket must be used within a SocketProvider");
  }
  return state;
};

export function SocketProvider({ children }: SocketProviderProps) {
  const [messages, setMessages] = useState<string[]>([]);
  const socketRef = useRef<Socket | null>(null);

  const sendMessage = useCallback<ISocketContext["sendMessage"]>((msg) => {
    socketRef.current?.emit("event:message", { message: msg });
  }, []);

  const onMessageReceived = useCallback((message: string) => {
    console.log("Message received:", message);
    setMessages((prevMessages) => [...prevMessages, message]);
  }, []);

  useEffect(() => {
    const socket = io(
      process.env.NEXT_PUBLIC_SOCKET_URL ?? "http://localhost:8000",
    );
    socket.on("message", onMessageReceived);
    socketRef.current = socket;
    return () => {
      socket.off("message", onMessageReceived);
      socket.disconnect();
      socketRef.current = null;
    };
  }, [onMessageReceived]);

  return (
    <SocketContext.Provider value={{ sendMessage, messages }}>
      {children}
    </SocketContext.Provider>
  );
}
