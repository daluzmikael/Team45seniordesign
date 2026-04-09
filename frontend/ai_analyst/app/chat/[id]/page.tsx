"use client"

import { useEffect, useRef, useState, useCallback } from "react"
import { useParams } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Textarea } from "@/components/ui/textarea"
import { Send } from "lucide-react"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"
import { useAuth } from "@/context/auth-context"
import { guestStorage } from "@/lib/guest-storage"

type Message = { role: "user" | "assistant"; content: string }

export default function ChatPage() {
  const { id: conversationId } = useParams<{ id: string }>()
  const { user } = useAuth()
  const [message, setMessage] = useState("")
  const [messages, setMessages] = useState<Message[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const bottomRef = useRef<HTMLDivElement>(null)
  const initializedRef = useRef<string | null>(null) // tracks which conversationId we've loaded

  const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"

  // saves convo messages to backend if logged in, otherwise to sessionStorage
  const saveToBackend = useCallback(async (role: "user" | "assistant", content: string) => {
    if (!user?.token || !conversationId) return
    await fetch(`${API_URL}/api/history/message`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${user.token}`,
      },
      body: JSON.stringify({ conversationId, role, content }),
    })
    // notify sidebar to refresh after every save so new chats appear immediately
    window.dispatchEvent(new Event("conversation-updated"))
  }, [user?.token, conversationId, API_URL])

  // handles sending a query and receiving the response
  const sendMessage = useCallback(async (text: string) => {
    const userMessage = text.trim()
    if (!userMessage || isLoading) return

    setMessages((prev) => [...prev, { role: "user", content: userMessage }])
    setMessage("")
    setIsLoading(true)
    void saveToBackend("user", userMessage)

    try {
      const headers: HeadersInit = { "Content-Type": "application/json" }
      if (user?.token) headers.Authorization = `Bearer ${user.token}`

      const res = await fetch(`${API_URL}/api/analysis`, {
        method: "POST",
        headers,
        body: JSON.stringify({ question: userMessage, conversationId }),
      })

      if (!res.ok) {
        const err = await res.json()
        throw new Error(err.detail || "Failed to get response")
      }

      const data = await res.json()
      const assistantContent = data.analysis || "No analysis available."
      setMessages((prev) => [...prev, { role: "assistant", content: assistantContent }])
      void saveToBackend("assistant", assistantContent)
    } catch (error) {
      const assistantContent = `Error: ${
        error instanceof Error ? error.message : "Failed to connect to backend"
      }`
      setMessages((prev) => [...prev, { role: "assistant", content: assistantContent }])
      void saveToBackend("assistant", assistantContent)
    } finally {
      setIsLoading(false)
    }
  }, [isLoading, user?.token, conversationId, API_URL, saveToBackend])

  // loads the conversation history
  useEffect(() => {
    if (!conversationId) return
    // if the convo was already initialized, this can be skipped
    if (initializedRef.current === conversationId) return
    initializedRef.current = conversationId

    const init = async () => {
      // 1. loads existing history
      let loaded: Message[] = []
      if (user?.token) {
        try {
          const res = await fetch(`${API_URL}/api/history/${conversationId}`, {
            headers: { Authorization: `Bearer ${user.token}` },
          })
          if (res.ok) {
            const data = await res.json()
            loaded = Array.isArray(data.messages)
              ? data.messages.filter(
                  (m: { role?: string; content?: string }) =>
                    (m.role === "user" || m.role === "assistant") &&
                    typeof m.content === "string"
                ).map((m: { role: "user" | "assistant"; content: string }) => ({
                  role: m.role,
                  content: m.content,
                }))
              : []
          }
        } catch (e) {
          console.error("Failed to load auth history:", e)
        }
      } else {
        loaded = guestStorage.getConversation(conversationId)
      }

      setMessages(loaded)

      // 2. after history is loaded, check if there is a new chat prefill
      const prefill = sessionStorage.getItem(`prefill:${conversationId}`)
      if (prefill) {
        sessionStorage.removeItem(`prefill:${conversationId}`)
        // only auto-send if there is no existing history, to avoid overwriting conversations with prefill examples
        if (loaded.length === 0) {
          sendMessage(prefill)
        }
      }
    }

    init()
  }, [conversationId, user?.token, API_URL, sendMessage])

  // guest conversations saved on message change
  useEffect(() => {
    if (!user && conversationId && messages.length > 0) {
      guestStorage.saveConversation(conversationId, messages)
      window.dispatchEvent(new Event("conversation-updated"))
    }
  }, [messages, user, conversationId])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" })
  }, [messages, isLoading])

  const handleSend = () => sendMessage(message)

  // for starting a new chat with a pre filled question from the homepage examples
  return (
    <div className="flex flex-col h-full max-w-4xl mx-auto">
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {messages.map((msg, idx) => (
          <div
            key={idx}
            className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
          >
            <div
              className={`rounded-lg px-4 py-2 max-w-[80%] ${
                msg.role === "user" ? "bg-primary text-primary-foreground" : "bg-muted"
              }`}
            >
              {msg.role === "assistant" ? (
                <ReactMarkdown
                  remarkPlugins={[remarkGfm]}
                  components={{
                    table: ({ children }) => (
                      <div className="overflow-x-auto my-2">
                        <table className="min-w-full border-collapse border border-border text-sm">
                          {children}
                        </table>
                      </div>
                    ),
                    thead: ({ children }) => <thead className="bg-muted/50">{children}</thead>,
                    th: ({ children }) => (
                      <th className="border border-border px-2 py-1 text-left font-semibold">
                        {children}
                      </th>
                    ),
                    td: ({ children }) => (
                      <td className="border border-border px-2 py-1">{children}</td>
                    ),
                    h3: ({ children }) => (
                      <h3 className="font-semibold underline underline-offset-4 mt-3 mb-2">
                        {children}
                      </h3>
                    ),
                    p: ({ children }) => <p className="whitespace-pre-wrap">{children}</p>,
                  }}
                >
                  {msg.content}
                </ReactMarkdown>
              ) : (
                <p className="whitespace-pre-wrap">{msg.content}</p>
              )}
            </div>
          </div>
        ))}

        {isLoading && (
          <div className="flex justify-start">
            <div className="rounded-lg px-4 py-2 bg-muted">
              <svg
                className="h-6 w-6 animate-spin text-primary"
                viewBox="0 0 24 24"
                fill="none"
                xmlns="http://www.w3.org/2000/svg"
              >
                <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="2" fill="currentColor" opacity="0.2"/>
                <path d="M12 2C13.1 2 14 2.9 14 4V8C14 9.1 13.1 10 12 10C10.9 10 10 9.1 10 8V4C10 2.9 10.9 2 12 2Z" fill="currentColor"/>
                <path d="M12 14C13.1 14 14 14.9 14 16V20C14 21.1 13.1 22 12 22C10.9 22 10 21.1 10 20V16C10 14.9 10.9 14 12 14Z" fill="currentColor"/>
                <path d="M20 12C21.1 12 22 12.9 22 14H18C18 12.9 18.9 12 20 12Z" fill="currentColor"/>
                <path d="M4 12C5.1 12 6 12.9 6 14H2C2 12.9 2.9 12 4 12Z" fill="currentColor"/>
                <path d="M16.24 7.76C17.07 8.59 17.07 9.93 16.24 10.76L14.83 9.35C15.22 8.96 15.22 8.35 14.83 7.96L16.24 7.76Z" fill="currentColor"/>
                <path d="M7.76 16.24C8.59 17.07 9.93 17.07 10.76 16.24L9.35 14.83C8.96 15.22 8.35 15.22 7.96 14.83L7.76 16.24Z" fill="currentColor"/>
                <path d="M16.24 16.24C15.41 17.07 14.07 17.07 13.24 16.24L14.65 14.83C15.04 15.22 15.65 15.22 16.04 14.83L16.24 16.24Z" fill="currentColor"/>
                <path d="M7.76 7.76C8.59 6.93 9.93 6.93 10.76 7.76L9.35 9.17C8.96 8.78 8.35 8.78 7.96 9.17L7.76 7.76Z" fill="currentColor"/>
              </svg>
            </div>
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      <div className="border-t bg-background p-4">
        <div className="flex gap-2 max-w-4xl mx-auto">
          <Textarea
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault()
                handleSend()
              }
            }}
            placeholder="Ask about basketball stats..."
            className="min-h-[60px] resize-none"
            disabled={isLoading}
          />
          <Button
            onClick={handleSend}
            size="icon"
            className="h-[60px] w-[60px]"
            disabled={isLoading || !message.trim()}
          >
            <Send className="h-5 w-5" />
          </Button>
        </div>
      </div>
    </div>
  )
}