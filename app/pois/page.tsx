import { MainLayout } from "@/components/layout/main-layout"
import { Button } from "@/components/ui/button"
import Link from "next/link"
import { Plus, Upload } from "lucide-react"
import { CollectionCard } from "@/components/pois/collection-card"

async function getPOICollections() {
  try {
    const { initConfigIfNeeded } = await import("@/lib/s3-config");
    const { initialPOICollectionsData } = await import("@/lib/seed-jobs");

    const collectionsData = await initConfigIfNeeded('poi-collections', initialPOICollectionsData);
    const collections = Object.values(collectionsData);

    return collections.map((col: any) => ({
      id: col.id,
      name: col.name,
      description: col.description,
      poi_count: col.poiCount || 0,
      sources: col.sources || {},
      created_at: col.createdAt,
      enrichedCount: col.enrichedCount || 0,
      enrichedAt: col.enrichedAt || null,
    }));
  } catch (error) {
    console.error("Error fetching POI collections:", error);
    return [];
  }
}

export default async function POIsPage() {
  const collections = await getPOICollections()

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

      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
        {collections.map((collection) => (
          <CollectionCard key={collection.id} collection={collection} />
        ))}
      </div>
    </MainLayout>
  )
}
