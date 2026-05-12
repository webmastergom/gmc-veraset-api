"use client"

import { useEffect, useMemo, useState } from "react"
import { useRouter } from "next/navigation"
import { MainLayout } from "@/components/layout/main-layout"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import Link from "next/link"
import { Plus, Upload, Loader2, Search, X } from "lucide-react"
import { CollectionCard } from "@/components/pois/collection-card"
import { useToast } from "@/hooks/use-toast"

interface POICollection {
  id: string
  name: string
  description: string
  poi_count: number
  sources?: Record<string, number | string>
  enrichedCount?: number
  enrichedAt?: string | null
}

export default function POIsPage() {
  const router = useRouter()
  const { toast } = useToast()
  const [collections, setCollections] = useState<POICollection[]>([])
  const [loading, setLoading] = useState(true)
  const [currentPage, setCurrentPage] = useState(1)
  const [query, setQuery] = useState("")
  const PAGE_SIZE = 20

  // Filter on name + description (case-insensitive). Memoized so we only
  // re-run when collections or the query change — not on every keystroke
  // re-render. Pagination is reset to page 1 below whenever the query
  // shrinks the set under the current page's offset.
  const filteredCollections = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return collections
    return collections.filter((c) =>
      c.name.toLowerCase().includes(q) ||
      (c.description || "").toLowerCase().includes(q)
    )
  }, [collections, query])

  // When the filter changes such that the current page would be empty,
  // snap back to page 1 — otherwise the user sees "Showing 41-60 of 3".
  useEffect(() => {
    const maxPage = Math.max(1, Math.ceil(filteredCollections.length / PAGE_SIZE))
    if (currentPage > maxPage) setCurrentPage(1)
  }, [filteredCollections.length, currentPage])

  const fetchCollections = async () => {
    try {
      const res = await fetch("/api/pois/collections", { cache: "no-store" })
      if (!res.ok) throw new Error("Failed to fetch collections")
      const data = await res.json()
      setCollections(
        data.map((col: any) => ({
          id: col.id,
          name: col.name,
          description: col.description,
          poi_count: col.poiCount || 0,
          sources: col.sources || {},
          enrichedCount: col.enrichedCount || 0,
          enrichedAt: col.enrichedAt || null,
        }))
      )
    } catch (error) {
      console.error("Error fetching POI collections:", error)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchCollections()
  }, [])

  const handleDelete = async (collectionId: string, collectionName: string) => {
    try {
      const res = await fetch(`/api/pois/collections/${collectionId}`, {
        method: "DELETE",
        credentials: "include",
      })

      if (!res.ok) {
        const err = await res.json()
        throw new Error(err.error || "Failed to delete collection")
      }

      setCollections((prev) => prev.filter((c) => c.id !== collectionId))
      toast({
        title: "Collection Deleted",
        description: `"${collectionName}" has been deleted.`,
      })
    } catch (error: any) {
      toast({
        title: "Delete Failed",
        description: error.message || "Failed to delete collection",
        variant: "destructive",
      })
    }
  }

  return (
    <MainLayout>
      <div className="mb-8 flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white">POI Collections</h1>
          <p className="text-gray-500 mt-2">
            Manage your Points of Interest collections
          </p>
        </div>
        <div className="flex space-x-2">
          <Link href="/pois/upload">
            <Button variant="outline">
              <Upload className="mr-2 h-4 w-4" />
              Upload
            </Button>
          </Link>
          <Link href="/pois/import">
            <Button>
              <Plus className="mr-2 h-4 w-4" />
              Import
            </Button>
          </Link>
        </div>
      </div>

      {/* Search box — filters by name + description, case-insensitive.
          Shows immediately under the header so it's visible even before
          the collections finish loading (the box is functional once they
          arrive). */}
      {!loading && collections.length > 0 && (
        <div className="mb-6 relative">
          <Search className="h-4 w-4 absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none" />
          <Input
            type="search"
            placeholder={`Search ${collections.length} collection${collections.length === 1 ? '' : 's'} by name or description…`}
            value={query}
            onChange={(e) => {
              setQuery(e.target.value)
              setCurrentPage(1)
            }}
            className="pl-9 pr-9"
            aria-label="Search POI collections"
          />
          {query && (
            <button
              type="button"
              onClick={() => { setQuery(""); setCurrentPage(1) }}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
              aria-label="Clear search"
            >
              <X className="h-4 w-4" />
            </button>
          )}
        </div>
      )}

      {(() => {
        const totalPages = Math.max(1, Math.ceil(filteredCollections.length / PAGE_SIZE));
        const paginatedCollections = filteredCollections.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE);

        return loading ? (
          <div className="flex items-center justify-center py-16">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </div>
        ) : collections.length === 0 ? (
          <div className="text-center py-16 text-muted-foreground">
            <p className="text-lg">No POI collections yet</p>
            <p className="text-sm mt-2">Upload or import POIs to get started</p>
          </div>
        ) : filteredCollections.length === 0 ? (
          <div className="text-center py-16 text-muted-foreground">
            <p className="text-lg">No collections match &ldquo;{query}&rdquo;</p>
            <p className="text-sm mt-2">
              Try a shorter or different keyword, or{' '}
              <button
                type="button"
                className="underline hover:text-foreground"
                onClick={() => { setQuery(""); setCurrentPage(1) }}
              >
                clear the search
              </button>
              .
            </p>
          </div>
        ) : (
          <>
            <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
              {paginatedCollections.map((collection) => (
                <CollectionCard
                  key={collection.id}
                  collection={collection}
                  onDelete={handleDelete}
                />
              ))}
            </div>
            {totalPages > 1 && (
              <div className="flex items-center justify-between mt-4">
                <p className="text-sm text-muted-foreground">
                  Showing {(currentPage - 1) * PAGE_SIZE + 1}-{Math.min(currentPage * PAGE_SIZE, filteredCollections.length)} of {filteredCollections.length}
                  {query && (
                    <span className="ml-1 opacity-70">(filtered from {collections.length})</span>
                  )}
                </p>
                <div className="flex items-center gap-2">
                  <Button variant="outline" size="sm" onClick={() => setCurrentPage((p) => Math.max(1, p - 1))} disabled={currentPage === 1}>Previous</Button>
                  <span className="text-sm text-muted-foreground px-2">Page {currentPage} of {totalPages}</span>
                  <Button variant="outline" size="sm" onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))} disabled={currentPage === totalPages}>Next</Button>
                </div>
              </div>
            )}
          </>
        );
      })()}
    </MainLayout>
  )
}
