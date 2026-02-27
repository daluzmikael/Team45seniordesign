"use client"

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
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu"
import { Avatar, AvatarFallback } from "@/components/ui/avatar"
import Link from "next/link"
import { useAuth } from "@/context/auth-context"

// Just for showcase, dud code
const chatHistory = [
  {
    id: "1",
    title: "Who is a better 3 point shooter between Jordan Poole and Dalton Knecht?",
    date: "Today",
  },
  {
    id: "2",
    title: "How many assists did Derrick Rose have at his last game?",
    date: "Today",
  },
  {
    id: "3",
    title: "Is Brian Scalabrine better than Lebron James?",
    date: "Yesterday",
  },
  {
    id: "4",
    title: "All time GSW Team",
    date: "Yesterday",
  },
  {
    id: "5",
    title: "Which Lakers year had the best stats",
    date: "Previous 7 Days",
  },
]

const groupedChats = chatHistory.reduce(
  (acc, chat) => {
    if (!acc[chat.date]) {
      acc[chat.date] = []
    }
    acc[chat.date].push(chat)
    return acc
  },
  {} as Record<string, typeof chatHistory>,
)

// Get initials from email e.g. "john@gmail.com" -> "JO"
function getInitials(email: string) {
  return email.slice(0, 2).toUpperCase()
}

export function AppSidebar() {
  const { user, logout } = useAuth()

  return (
    <Sidebar>
      <SidebarHeader className="border-b border-sidebar-border p-4">
        <Button className="w-full justify-start gap-2 bg-transparent" variant="outline">
          <MessageSquarePlus className="h-4 w-4" />
          New Chat
        </Button>
      </SidebarHeader>

      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              <SidebarMenuItem>
                <SidebarMenuButton>
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
                <SidebarMenuButton asChild>
                  <Link href="/dashboards">
                    <LayoutDashboard className="h-4 w-4" />
                    <span>Dashboards</span>
                  </Link>
                </SidebarMenuButton>
              </SidebarMenuItem>
              <SidebarMenuItem>
                <SidebarMenuButton asChild>
                  <Link href="/">
                    <MessageCircle className="h-4 w-4" />
                    <span>Analyst</span>
                  </Link>
                </SidebarMenuButton>
              </SidebarMenuItem>
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarSeparator />

        {Object.entries(groupedChats).map(([date, chats]) => (
          <SidebarGroup key={date}>
            <SidebarGroupLabel>{date}</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                {chats.map((chat) => (
                  <SidebarMenuItem key={chat.id}>
                    <SidebarMenuButton>
                      <MessageSquarePlus className="h-4 w-4" />
                      <span className="truncate">{chat.title}</span>
                    </SidebarMenuButton>
                    <DropdownMenu>
                      <DropdownMenuTrigger asChild>
                        <SidebarMenuAction>
                          <MoreHorizontal className="h-4 w-4" />
                          <span className="sr-only">More</span>
                        </SidebarMenuAction>
                      </DropdownMenuTrigger>
                      <DropdownMenuContent side="right" align="start">
                        <DropdownMenuItem>
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

      <SidebarFooter className="border-t border-sidebar-border p-4">
        <SidebarMenu>
          <SidebarMenuItem>
            {user ? (
              // Logged in state
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <SidebarMenuButton className="w-full">
                    <Avatar className="h-6 w-6">
                      <AvatarFallback className="text-xs">{getInitials(user.email)}</AvatarFallback>
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
                  <DropdownMenuItem onClick={logout} className="text-destructive focus:text-destructive">
                    <LogOut className="h-4 w-4 mr-2" />
                    Sign Out
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            ) : (
              // Guest state
              <SidebarMenuButton asChild>
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