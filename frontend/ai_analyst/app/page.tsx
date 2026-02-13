"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Textarea } from "@/components/ui/textarea"
import { Send } from "lucide-react"

export default function Home() {
  const [message, setMessage] = useState("")
  const [messages, setMessages] = useState<Array<{ role: "user" | "assistant"; content: string }>>([])
  const [isLoading, setIsLoading] = useState(false)

  const handleSend = async () => {
    if (!message.trim()) return

    const userMessage = message
    setMessages([...messages, { role: "user", content: userMessage }])
    setMessage("")
    setIsLoading(true)

    try {
      const response = await fetch("http://localhost:8000/api/analysis", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ question: userMessage }),
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || "Failed to get response")
      }

      const data = await response.json()
      
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: data.analysis || "No analysis available.",
        },
      ])
    } catch (error) {
      console.error("Error:", error)
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: `Error: ${error instanceof Error ? error.message : "Failed to get response from server"}`,
        },
      ])
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
            <h1 className="text-4xl font-bold mb-4">Basketball Analyst</h1>
            <p className="text-muted-foreground text-lg mb-8">
              Ask me anything about basketball stats, players, or teams
            </p>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 w-full max-w-2xl">
              <div 
                className="rounded-lg border bg-card p-4 hover:bg-accent cursor-pointer transition-colors"
                onClick={() => handleExampleClick("Compare LeBron and Jordan's career stats")}
              >
                <p className="text-sm">Compare LeBron and Jordan's career stats</p>
              </div>
              <div 
                className="rounded-lg border bg-card p-4 hover:bg-accent cursor-pointer transition-colors"
                onClick={() => handleExampleClick("Show me Steph Curry's 3-point percentage by season")}
              >
                <p className="text-sm">Show me Steph Curry's 3-point percentage by season</p>
              </div>
              <div 
                className="rounded-lg border bg-card p-4 hover:bg-accent cursor-pointer transition-colors"
                onClick={() => handleExampleClick("What are the Lakers' win-loss records this season?")}
              >
                <p className="text-sm">What are the Lakers' win-loss records this season?</p>
              </div>
              <div 
                className="rounded-lg border bg-card p-4 hover:bg-accent cursor-pointer transition-colors"
                onClick={() => handleExampleClick("Analyze Giannis' playoff performance")}
              >
                <p className="text-sm">Analyze Giannis' playoff performance</p>
              </div>
            </div>
          </div>
        ) : (
          messages.map((msg, idx) => (
            <div key={idx} className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}>
              <div
                className={`rounded-lg px-4 py-2 max-w-[80%] whitespace-pre-wrap ${
                  msg.role === "user" ? "bg-primary text-primary-foreground" : "bg-muted"
                }`}
              >
                {msg.content}
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