"use client"

import { useEffect, useState } from "react"
import { Button } from "@/components/ui/button"
import { Textarea } from "@/components/ui/textarea"
import { Send } from "lucide-react"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"
import { useAuth } from "@/context/auth-context"

export default function Home() {
  const { user } = useAuth()
  const [message, setMessage] = useState("")
  const [messages, setMessages] = useState<
    Array<{ role: "user" | "assistant"; content: string }>
  >([])
  const [isLoading, setIsLoading] = useState(false)
  const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"
  const DEFAULT_CONVERSATION_ID = "default"

  const saveMessageToHistory = async (
    role: "user" | "assistant",
    content: string
  ) => {
    if (!user?.token) return
    await fetch(`${API_URL}/api/history/message`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${user.token}`,
      },
      body: JSON.stringify({
        conversationId: DEFAULT_CONVERSATION_ID,
        role,
        content,
      }),
    })
  }

  useEffect(() => {
    const loadHistory = async () => {
      if (!user?.token) return

      try {
        const response = await fetch(
          `${API_URL}/api/history/${DEFAULT_CONVERSATION_ID}`,
          {
            headers: {
              Authorization: `Bearer ${user.token}`,
            },
          }
        )
        if (!response.ok) return

        const data = await response.json()
        const historyMessages: Array<{ role: "user" | "assistant"; content: string }> =
          Array.isArray(data.messages)
            ? data.messages
                .filter((m: { role?: string; content?: string }) =>
                  (m.role === "user" || m.role === "assistant") && typeof m.content === "string"
                )
                .map((m: { role: "user" | "assistant"; content: string }) => ({
                  role: m.role,
                  content: m.content,
                }))
            : []
        setMessages(historyMessages)
      } catch (error) {
        console.error("Failed to load history:", error)
      }
    }

    loadHistory()
  }, [API_URL, user?.token])

  const handleSend = async () => {
    if (!message.trim()) return

    const userMessage = message.trim()
    setMessages((prev) => [...prev, { role: "user", content: userMessage }])
    setMessage("")
    setIsLoading(true)
    void saveMessageToHistory("user", userMessage)

    try {
      const analysisHeaders: HeadersInit = {
        "Content-Type": "application/json",
      }
      if (user?.token) {
        analysisHeaders.Authorization = `Bearer ${user.token}`
      }

      const response = await fetch(`${API_URL}/api/analysis`, {
        method: "POST",
        headers: analysisHeaders,
        body: JSON.stringify({
          question: userMessage,
          conversationId: DEFAULT_CONVERSATION_ID,
        }),
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || "Failed to get response")
      }

      const data = await response.json()
      const assistantContent = data.analysis || "No analysis available."

      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: assistantContent,
        },
      ])
      void saveMessageToHistory("assistant", assistantContent)
    } catch (error) {
      console.error("Error:", error)

      const assistantContent = `Error: ${
        error instanceof Error
          ? error.message
          : "Failed to connect to backend"
      }`

      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: assistantContent,
        },
      ])
      void saveMessageToHistory("assistant", assistantContent)
    } finally {
      setIsLoading(false)
    }
  }

  const handleExampleClick = (exampleText: string) => {
    setMessage(exampleText)
  }

  return (
    <div className="flex flex-col h-full max-w-4xl mx-auto">
      {/* Chat messages area */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {messages.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-center">
            <h1 className="text-4xl font-bold mb-4">
              Basketball Analyst
            </h1>
            <p className="text-muted-foreground text-lg mb-8">
              Ask me anything about basketball stats, players, or teams
            </p>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 w-full max-w-2xl">
              <div
                className="rounded-lg border bg-card p-4 hover:bg-accent cursor-pointer transition-colors"
                onClick={() =>
                  handleExampleClick(
                    "Compare LeBron and Jordan's career stats"
                  )
                }
              >
                <p className="text-sm">
                  Compare LeBron and Jordan's career stats
                </p>
              </div>

              <div
                className="rounded-lg border bg-card p-4 hover:bg-accent cursor-pointer transition-colors"
                onClick={() =>
                  handleExampleClick(
                    "Show me Steph Curry's 3-point percentage by season"
                  )
                }
              >
                <p className="text-sm">
                  Show me Steph Curry's 3-point percentage by season
                </p>
              </div>

              <div
                className="rounded-lg border bg-card p-4 hover:bg-accent cursor-pointer transition-colors"
                onClick={() =>
                  handleExampleClick(
                    "What are the Lakers' win-loss records this season?"
                  )
                }
              >
                <p className="text-sm">
                  What are the Lakers' win-loss records this season?
                </p>
              </div>

              <div
                className="rounded-lg border bg-card p-4 hover:bg-accent cursor-pointer transition-colors"
                onClick={() =>
                  handleExampleClick(
                    "Analyze Giannis' playoff performance"
                  )
                }
              >
                <p className="text-sm">
                  Analyze Giannis' playoff performance
                </p>
              </div>
            </div>
          </div>
        ) : (
          messages.map((msg, idx) => (
            <div
              key={idx}
              className={`flex ${
                msg.role === "user"
                  ? "justify-end"
                  : "justify-start"
              }`}
            >
              <div
                className={`rounded-lg px-4 py-2 max-w-[80%] ${
                  msg.role === "user"
                    ? "bg-primary text-primary-foreground"
                    : "bg-muted"
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
                        <th className="border border-border px-2 py-1 text-left font-semibold">{children}</th>
                      ),
                      td: ({ children }) => <td className="border border-border px-2 py-1">{children}</td>,
                      h3: ({ children }) => (
                        <h3 className="font-semibold underline underline-offset-4 mt-3 mb-2">{children}</h3>
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
          ))
        )}

        {isLoading && (
          <div className="flex justify-start">
            <div className="rounded-lg px-4 py-2 bg-muted">
              <p className="animate-pulse">Analyzing...</p>
            </div>
          </div>
        )}
      </div>

      {/* Chat input area */}
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