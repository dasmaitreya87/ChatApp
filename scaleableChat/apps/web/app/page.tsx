"use client";
import { useSocket } from '../context/SocketProvider'
import { useState } from 'react'
import classes from './page.module.css'
export default function Page() {
  const { sendMessage, messages } = useSocket();
  const [message, setMessage] = useState("");
  return (
    <div>
      <div>
        <input
          className={classes.chatInput}
          placeholder="Message..."
          value={message}
          onChange={(e) => setMessage(e.target.value)}
        />
        <button className={classes.button} onClick={() => sendMessage(message)}>
          Send
        </button>
      </div>
      <div>
        <h2>Messages:</h2>
        <ul>
          {messages.map((msg, index) => (
            <li key={index}>{msg}</li>
          ))}
        </ul>
      </div>
    </div>
  );
}