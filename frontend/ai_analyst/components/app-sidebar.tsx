"use client"

import { useEffect, useState, useCallback } from "react"
import { useRouter, useParams } from "next/navigation"
import {
  MessageSquarePlus,
  Search,
  Settings,
  User,
  MoreHorizontal,
  Trash2,
  LayoutDashboard,
  MessageCircle,
  LogOut,
  LogIn,
} from "lucide-react"
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuAction,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarSeparator,
} from "@/components/ui/sidebar"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Avatar, AvatarFallback } from "@/components/ui/avatar"
import Link from "next/link"
import { useAuth } from "@/context/auth-context"
import { guestStorage, type GuestConversationMeta } from "@/lib/guest-storage"

// types
type ConversationMeta = {
  conversationId: string
  latestMessageAt: number
  title?: string
}

// helpers 
function getInitials(email: string) {
  return email.slice(0, 2).toUpperCase()
}

function groupByDate(conversations: ConversationMeta[]): Record<string, ConversationMeta[]> {
  const now = Date.now()
  const ONE_DAY = 86_400_000
  const SEVEN_DAYS = 7 * ONE_DAY

  const groups: Record<string, ConversationMeta[]> = {}
  for (const conv of conversations) {
    const age = now - (conv.latestMessageAt || 0)
    const label =
      age < ONE_DAY ? "Today" : age < 2 * ONE_DAY ? "Yesterday" : age < SEVEN_DAYS ? "Previous 7 Days" : "Older"
    if (!groups[label]) groups[label] = []
    groups[label].push(conv)
  }
  return groups
}

const DATE_ORDER = ["Today", "Yesterday", "Previous 7 Days", "Older"]

// component
export function AppSidebar() {
  const { user, logout } = useAuth()
  const router = useRouter()
  const params = useParams<{ id?: string }>()
  const activeId = params?.id

  const [conversations, setConversations] = useState<ConversationMeta[]>([])
  const [loadingHistory, setLoadingHistory] = useState(false)

  const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"

  // loads conversation list from backend firebase/sessionStorage 
  const loadConversations = useCallback(async () => {
    if (user?.token) {
      setLoadingHistory(true)
      try {
        const res = await fetch(`${API_URL}/api/history`, {
          headers: { Authorization: `Bearer ${user.token}` },
        })
        if (res.ok) {
          const data = await res.json()
          // Backend now returns title from the first user message
          const list: ConversationMeta[] = Array.isArray(data.conversations)
            ? data.conversations.map((c: { conversationId: string; latestMessageAt: number; title?: string }) => ({
                conversationId: c.conversationId,
                latestMessageAt: c.latestMessageAt,
                title: c.title,
              }))
            : []
          setConversations(list)
        }
      } catch (e) {
        console.error("Failed to load conversation list:", e)
      } finally {
        setLoadingHistory(false)
      }
    } else {
      // for guests, list conversations from sessionStorage
      const list: ConversationMeta[] = guestStorage.listConversations().map(
        (c: GuestConversationMeta) => ({
          conversationId: c.conversationId,
          latestMessageAt: c.latestMessageAt,
          title: c.title,
        })
      )
      setConversations(list)
    }
  }, [user?.token, API_URL])

  useEffect(() => {
    loadConversations()
  }, [loadConversations])

  // reload conversation list when user logs in/out
  useEffect(() => {
    if (!user) {
      loadConversations()
    }
  }, [activeId, user, loadConversations])

  // listen for new messages from the chat page and reload the sidebar live
  useEffect(() => {
    const handler = () => loadConversations()
    window.addEventListener("conversation-updated", handler)
    return () => window.removeEventListener("conversation-updated", handler)
  }, [loadConversations])

  // start a new chat with empty history
  const handleNewChat = () => {
    router.push("/")
  }

  // delete conversation (for anyone who looks at this, its just frontend for now, need to add delete endpoint for backend eventually firebase)
  const handleDelete = async (conversationId: string, e: React.MouseEvent) => {
    e.stopPropagation()
    if (user?.token) {
      // TODO: add a DELETE /api/history/:id endpoint on the backend
      // For now, just remove it from local state
      console.warn("backend delete hasn't been implemented yet")
    } else {
      guestStorage.deleteConversation(conversationId)
    }
    setConversations((prev) => prev.filter((c) => c.conversationId !== conversationId))
    if (activeId === conversationId) {
      router.push("/")
    }
  }

  // chat display title
  const getTitle = (conv: ConversationMeta) =>
    conv.title || `Chat ${conv.conversationId.slice(0, 8)}`

  const grouped = groupByDate(conversations)
  const tabClass =
    "border-0 bg-red-400 text-zinc-900 hover:bg-red-300 font-medium data-[active=true]:bg-red-400 data-[active=true]:text-zinc-900 data-[active=true]:hover:bg-red-300"
  const activeTabClass = "bg-red-400 text-zinc-900"
  const searchClass =
    "border-0 bg-[#bcc1c9] text-zinc-900 hover:bg-[#aeb4be] dark:bg-[#16171b] dark:text-zinc-100 dark:hover:bg-[#23252b]"
  const savedChatClass =
    "border-0 bg-[#bcc1c9] text-zinc-900 hover:bg-[#aeb4be] dark:bg-[#16171b] dark:text-zinc-100 dark:hover:bg-[#23252b] data-[active=true]:bg-[#bcc1c9] data-[active=true]:text-zinc-900 data-[active=true]:hover:bg-[#aeb4be] dark:data-[active=true]:bg-[#16171b] dark:data-[active=true]:text-zinc-100 dark:data-[active=true]:hover:bg-[#23252b]"

  return (
    <Sidebar className="[&_[data-sidebar=sidebar]]:bg-[#2f3136] [&_[data-sidebar=sidebar]]:text-zinc-100">
      <SidebarHeader className="p-2">
        <Button
          className={`w-full justify-start gap-2 ${tabClass}`}
          variant="default"
          onClick={handleNewChat}
        >
          <MessageSquarePlus className="h-4 w-4" />
          New Chat
        </Button>
      </SidebarHeader>

      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              <SidebarMenuItem>
                <SidebarMenuButton className={searchClass}>
                  <Search className="h-4 w-4" />
                  <span>Search</span>
                </SidebarMenuButton>
              </SidebarMenuItem>
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              <SidebarMenuItem>
                <SidebarMenuButton asChild className={tabClass}>
                  <Link href="/dashboards">
                    <LayoutDashboard className="h-4 w-4" />
                    <span>Dashboards</span>
                  </Link>
                </SidebarMenuButton>
              </SidebarMenuItem>
              <SidebarMenuItem>
                <SidebarMenuButton asChild className={tabClass}>
                  <Link href="/">
                    <MessageCircle className="h-4 w-4" />
                    <span>Analyst</span>
                  </Link>
                </SidebarMenuButton>
              </SidebarMenuItem>
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        {loadingHistory && (
          <SidebarGroup>
            <SidebarGroupContent>
              <p className="text-xs text-muted-foreground px-4 py-2 animate-pulse">
                Loading history…
              </p>
            </SidebarGroupContent>
          </SidebarGroup>
        )}

        {!loadingHistory && conversations.length === 0 && (
          <SidebarGroup>
            <SidebarGroupContent>
              <p className="text-xs text-muted-foreground px-4 py-2">No chats yet.</p>
            </SidebarGroupContent>
          </SidebarGroup>
        )}

        {DATE_ORDER.filter((label) => grouped[label]?.length).map((label) => (
          <SidebarGroup key={label}>
            <SidebarGroupLabel>{label}</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                {grouped[label].map((conv) => (
                  <SidebarMenuItem key={conv.conversationId}>
                    <SidebarMenuButton
                      asChild
                      isActive={activeId === conv.conversationId}
                      className={savedChatClass}
                    >
                      <Link href={`/chat/${conv.conversationId}`}>
                        <MessageSquarePlus className="h-4 w-4 shrink-0" />
                        <span className="truncate">{getTitle(conv)}</span>
                      </Link>
                    </SidebarMenuButton>
                    <DropdownMenu>
                      <DropdownMenuTrigger asChild>
                        <SidebarMenuAction>
                          <MoreHorizontal className="h-4 w-4" />
                          <span className="sr-only">More</span>
                        </SidebarMenuAction>
                      </DropdownMenuTrigger>
                      <DropdownMenuContent side="right" align="start">
                        <DropdownMenuItem
                          onClick={(e) => handleDelete(conv.conversationId, e)}
                          className="text-destructive focus:text-destructive"
                        >
                          <Trash2 className="h-4 w-4 mr-2" />
                          Delete
                        </DropdownMenuItem>
                      </DropdownMenuContent>
                    </DropdownMenu>
                  </SidebarMenuItem>
                ))}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        ))}
      </SidebarContent>

      <SidebarFooter className="p-4">
        <SidebarMenu>
          <SidebarMenuItem>
            {user ? (
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <SidebarMenuButton className={`w-full ${tabClass}`}>
                    <Avatar className="h-6 w-6">
                      <AvatarFallback className="text-xs">
                        {getInitials(user.email)}
                      </AvatarFallback>
                    </Avatar>
                    <span className="flex-1 text-left truncate">{user.email}</span>
                    <MoreHorizontal className="h-4 w-4" />
                  </SidebarMenuButton>
                </DropdownMenuTrigger>
                <DropdownMenuContent side="top" align="start" className="w-56">
                  <DropdownMenuItem>
                    <User className="h-4 w-4 mr-2" />
                    Profile
                  </DropdownMenuItem>
                  <DropdownMenuItem>
                    <Settings className="h-4 w-4 mr-2" />
                    Settings
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    onClick={logout}
                    className="text-destructive focus:text-destructive"
                  >
                    <LogOut className="h-4 w-4 mr-2" />
                    Sign Out
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            ) : (
              <SidebarMenuButton asChild className={tabClass}>
                <Link href="/login">
                  <LogIn className="h-4 w-4" />
                  <span>Sign In</span>
                </Link>
              </SidebarMenuButton>
            )}
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarFooter>
    </Sidebar>
  )
}