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
  const [loadingState, setLoadingState] = useState<string | null>(null)
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
    if (!userMessage || loadingState) return

    setMessages((prev) => [...prev, { role: "user", content: userMessage }])
    setMessage("")
    setLoadingState("querying")
    void saveToBackend("user", userMessage)

    try {
      const headers: HeadersInit = { "Content-Type": "application/json" }
      if (user?.token) headers.Authorization = `Bearer ${user.token}`

      setLoadingState("waiting")
      const res = await fetch(`${API_URL}/api/analysis`, {
        method: "POST",
        headers,
        body: JSON.stringify({ question: userMessage, conversationId }),
      })

      if (!res.ok) {
        const err = await res.json()
        throw new Error(err.detail || "Failed to get response")
      }

      setLoadingState("processing")
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
      setLoadingState(null)
    }
  }, [loadingState, user?.token, conversationId, API_URL, saveToBackend])

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
  }, [messages, loadingState])

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

        {loadingState && (
          <div className="flex justify-start">
            <div className="rounded-lg px-4 py-2 bg-muted">
              <p className="animate-pulse">
                {loadingState === "querying" && "Querying database..."}
                {loadingState === "waiting" && "Waiting for response..."}
                {loadingState === "processing" && "Processing results..."}
              </p>
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
            disabled={!!loadingState}
          />
          <Button
            onClick={handleSend}
            size="icon"
            className="h-[60px] w-[60px]"
            disabled={!!loadingState || !message.trim()}
          >
            <Send className="h-5 w-5" />
          </Button>
        </div>
      </div>
    </div>
  )
}