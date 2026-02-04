import { MainLayout } from "@/components/layout/main-layout"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import Link from "next/link"
import { Plus, Upload, MapPin } from "lucide-react"

async function getPOICollections() {
  try {
    const { getConfig, initConfigIfNeeded } = await import("@/lib/s3-config");
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
          <Card key={collection.id} className="hover:shadow-lg transition-shadow">
            <CardHeader>
              <div className="flex items-start justify-between">
                <div className="flex-1">
                  <CardTitle>{collection.name}</CardTitle>
                  <CardDescription className="mt-2">
                    {collection.description}
                  </CardDescription>
                </div>
                <MapPin className="h-5 w-5 text-muted-foreground" />
              </div>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div>
                  <p className="text-2xl font-bold">{collection.poi_count.toLocaleString()}</p>
                  <p className="text-sm text-muted-foreground">POIs</p>
                </div>
                
                {collection.sources && (
                  <div className="space-y-1">
                    <p className="text-sm font-medium">Sources:</p>
                    <div className="flex flex-wrap gap-2">
                      {Object.entries(collection.sources).map(([source, count]) => (
                        <span key={source} className="text-xs bg-secondary px-2 py-1 rounded">
                          {source}: {typeof count === 'number' ? count : String(count)}
                        </span>
                      ))}
                    </div>
                  </div>
                )}

                <div className="flex space-x-2 pt-2">
                  <Link href={`/pois/${collection.id}`} className="flex-1">
                    <Button variant="outline" className="w-full">
                      View Details
                    </Button>
                  </Link>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
    </MainLayout>
  )
}
