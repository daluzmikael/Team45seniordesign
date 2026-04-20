"use client"

import { useState } from "react"
import { useRouter } from "next/navigation"
import { v4 as uuidv4 } from "uuid"
import { Button } from "@/components/ui/button"
import { Textarea } from "@/components/ui/textarea"
import { Send } from "lucide-react"

export default function Home() {
  const router = useRouter()
  const [message, setMessage] = useState("")

  const startChat = (question?: string) => {
    const text = (question ?? message).trim()
    if (!text) return
    const newId = uuidv4()
    sessionStorage.setItem(`prefill:${newId}`, text)
    router.push(`/chat/${newId}`)
  }

  return (
    <div className="flex flex-col h-full max-w-4xl mx-auto">
      <div className="flex-1 flex flex-col items-center justify-center text-center px-4">
        <h1 className="text-4xl font-bold mb-4">Basketball Analyst</h1>
        <p className="text-muted-foreground text-lg mb-8">
          Ask me anything about basketball stats, players, or teams
        </p>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 w-full max-w-2xl">
          {[
            "Compare LeBron and Jordan's career stats",
            "Show me Steph Curry's 3-point percentage by season",
            "What are the Lakers' win-loss records this season?",
            "Analyze Giannis' playoff performance",
          ].map((example) => (
            <div
              key={example}
              className="cursor-pointer rounded-lg border border-red-400 bg-red-500 p-4 text-zinc-100 transition-colors hover:bg-red-400"
              onClick={() => setMessage(example)}
            >
              <p className="text-sm">{example}</p>
            </div>
          ))}
        </div>
      </div>

      <div className="border-t border-zinc-500 bg-[#c7cad1] p-4 dark:border-zinc-800 dark:bg-[#1c1d21]">
        <div className="mx-auto flex max-w-4xl gap-2">
          <Textarea
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault()
                startChat()
              }
            }}
            placeholder="Ask about basketball stats..."
            className="min-h-[60px] resize-none border-zinc-500 bg-[#c7cad1] text-zinc-900 placeholder:text-zinc-600 dark:border-zinc-700 dark:bg-[#1c1d21] dark:text-zinc-100 dark:placeholder:text-zinc-400"
          />
          <Button
            onClick={() => startChat()}
            size="icon"
            className="h-[60px] w-[60px] border border-red-400 bg-red-500 text-zinc-100 hover:bg-red-400"
            disabled={!message.trim()}
          >
            <Send className="h-5 w-5 text-zinc-600" />
          </Button>
        </div>
      </div>
    </div>
  )
}