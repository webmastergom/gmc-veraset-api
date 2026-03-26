"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { MainLayout } from "@/components/layout/main-layout"
import { Button } from "@/components/ui/button"
import Link from "next/link"
import { Plus, Upload, Loader2 } from "lucide-react"
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
  const PAGE_SIZE = 20

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

      {(() => {
        const totalPages = Math.ceil(collections.length / PAGE_SIZE);
        const paginatedCollections = collections.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE);

        return loading ? (
          <div className="flex items-center justify-center py-16">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </div>
        ) : collections.length === 0 ? (
          <div className="text-center py-16 text-muted-foreground">
            <p className="text-lg">No POI collections yet</p>
            <p className="text-sm mt-2">Upload or import POIs to get started</p>
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
                  Showing {(currentPage - 1) * PAGE_SIZE + 1}-{Math.min(currentPage * PAGE_SIZE, collections.length)} of {collections.length}
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
