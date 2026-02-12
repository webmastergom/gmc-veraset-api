"use client"

import { useState, useEffect, useCallback } from "react"
import { useParams } from "next/navigation"
import { MainLayout } from "@/components/layout/main-layout"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { useToast } from "@/hooks/use-toast"
import { ArrowLeft, Save, Edit2, X, Check, MapPin, Loader2, AlertCircle } from "lucide-react"
import Link from "next/link"

interface POIFeature {
  type: "Feature"
  id?: string | number
  geometry: {
    type: "Point"
    coordinates: [number, number] // [longitude, latitude]
  }
  properties: Record<string, any>
}

interface GeoJSON {
  type: "FeatureCollection"
  features: POIFeature[]
}

export default function POICollectionPage() {
  const params = useParams()
  const { toast } = useToast()
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [geojson, setGeojson] = useState<GeoJSON | null>(null)
  const [collectionName, setCollectionName] = useState("")
  const [editingIndex, setEditingIndex] = useState<number | null>(null)
  const [editedProperties, setEditedProperties] = useState<Record<string, any> | null>(null)
  const [searchTerm, setSearchTerm] = useState("")
  const [filteredFeatures, setFilteredFeatures] = useState<POIFeature[]>([])

  const collectionId = params.id as string

  const loadCollection = useCallback(async () => {
    try {
      setLoading(true)
      const response = await fetch(`/api/pois/collections/${collectionId}/geojson`)
      
      if (!response.ok) {
        throw new Error(`Failed to load collection: ${response.statusText}`)
      }

      const data: GeoJSON = await response.json()
      setGeojson(data)
      
      // Get collection name from metadata
      let nameToSet = collectionId
      try {
        const collectionsResponse = await fetch('/api/pois/collections')
        if (collectionsResponse.ok) {
          const collections = await collectionsResponse.json()
          const collection = collections.find((c: any) => c.id === collectionId)
          if (collection?.name) {
            nameToSet = collection.name
          }
        }
      } catch (e) {
        // Ignore metadata fetch errors
      }
      setCollectionName(nameToSet)
    } catch (error: any) {
      console.error("Error loading collection:", error)
      toast({
        title: "Error",
        description: error.message || "Failed to load POI collection",
        variant: "destructive",
      })
    } finally {
      setLoading(false)
    }
  // collectionName is set inside loadCollection, omit to avoid extra runs
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [collectionId, toast])

  const filterFeatures = useCallback(() => {
    if (!geojson) return

    if (!searchTerm.trim()) {
      setFilteredFeatures(geojson.features)
      return
    }

    const term = searchTerm.toLowerCase()
    const filtered = geojson.features.filter((feature) => {
      const props = feature.properties || {}
      const searchableText = [
        feature.id?.toString(),
        props.name,
        props.id,
        props.poi_id,
        props.address,
        props.city,
        props.category,
        props.brand,
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase()

      return searchableText.includes(term)
    })

    setFilteredFeatures(filtered)
  }, [geojson, searchTerm])

  useEffect(() => {
    loadCollection()
  }, [loadCollection])

  useEffect(() => {
    if (geojson) {
      filterFeatures()
    }
  }, [searchTerm, geojson, filterFeatures])

  const startEditing = (index: number) => {
    const feature = filteredFeatures[index]
    setEditingIndex(index)
    setEditedProperties({ ...feature.properties })
  }

  const cancelEditing = () => {
    setEditingIndex(null)
    setEditedProperties(null)
  }

  const saveEdit = () => {
    if (editingIndex === null || !editedProperties || !geojson) return

    const originalIndex = geojson.features.findIndex(
      (f) => f === filteredFeatures[editingIndex]
    )

    if (originalIndex === -1) return

    const updatedGeojson = { ...geojson }
    updatedGeojson.features[originalIndex] = {
      ...updatedGeojson.features[originalIndex],
      properties: editedProperties,
    }

    setGeojson(updatedGeojson)
    setEditingIndex(null)
    setEditedProperties(null)
    
    toast({
      title: "Updated",
      description: "POI properties updated (remember to save changes)",
    })
  }

  const updateProperty = (key: string, value: any) => {
    if (!editedProperties) return
    setEditedProperties({
      ...editedProperties,
      [key]: value,
    })
  }

  const saveCollection = async () => {
    if (!geojson) return

    if (!collectionName.trim()) {
      toast({
        title: "Name required",
        description: "Please enter a name for the collection",
        variant: "destructive",
      })
      return
    }

    try {
      setSaving(true)

      // Update collection metadata (name, description)
      const metaRes = await fetch(`/api/pois/collections/${collectionId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: collectionName.trim() }),
      })
      if (!metaRes.ok) {
        const err = await metaRes.json()
        throw new Error(err.error || "Failed to update collection name")
      }

      // Update GeoJSON
      const response = await fetch(`/api/pois/collections/${collectionId}/geojson`, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(geojson),
      })

      if (!response.ok) {
        const error = await response.json()
        throw new Error(error.error || "Failed to save collection")
      }

      toast({
        title: "Success",
        description: "POI collection saved successfully",
      })

      // Reload to get updated data
      await loadCollection()
    } catch (error: any) {
      console.error("Error saving collection:", error)
      toast({
        title: "Error",
        description: error.message || "Failed to save collection",
        variant: "destructive",
      })
    } finally {
      setSaving(false)
    }
  }

  const getPropertyValue = (feature: POIFeature, key: string): string => {
    return feature.properties?.[key]?.toString() || ""
  }

  const getPropertyKeys = (): string[] => {
    if (!geojson || geojson.features.length === 0) return []
    
    const allKeys = new Set<string>()
    geojson.features.forEach((feature) => {
      Object.keys(feature.properties || {}).forEach((key) => {
        allKeys.add(key)
      })
    })
    
    return Array.from(allKeys).sort()
  }

  if (loading) {
    return (
      <MainLayout>
        <div className="flex items-center justify-center min-h-[400px]">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      </MainLayout>
    )
  }

  if (!geojson) {
    return (
      <MainLayout>
        <Card>
          <CardContent className="pt-6">
            <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>
                Failed to load POI collection. Please try again.
              </AlertDescription>
            </Alert>
            <div className="mt-4">
              <Link href="/pois">
                <Button variant="outline">
                  <ArrowLeft className="mr-2 h-4 w-4" />
                  Back to Collections
                </Button>
              </Link>
            </div>
          </CardContent>
        </Card>
      </MainLayout>
    )
  }

  const propertyKeys = getPropertyKeys()
  const featuresToShow = filteredFeatures.length > 0 ? filteredFeatures : geojson.features

  return (
    <MainLayout>
      <div className="mb-6">
        <Link href="/pois">
          <Button variant="ghost" className="mb-4">
            <ArrowLeft className="mr-2 h-4 w-4" />
            Back to Collections
          </Button>
        </Link>
        <div className="flex items-center justify-between">
          <div className="flex-1 min-w-0 mr-4">
            <Input
              value={collectionName}
              onChange={(e) => setCollectionName(e.target.value)}
              className="text-3xl font-bold h-auto py-2 border-0 border-b-2 border-transparent hover:border-muted-foreground/30 focus:border-primary focus-visible:ring-0 focus-visible:ring-offset-0 bg-transparent"
              placeholder="Collection name"
            />
            <p className="text-muted-foreground mt-2">
              {geojson.features.length} POIs
            </p>
          </div>
          <Button onClick={saveCollection} disabled={saving}>
            {saving ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Saving...
              </>
            ) : (
              <>
                <Save className="mr-2 h-4 w-4" />
                Save Changes
              </>
            )}
          </Button>
        </div>
      </div>

      <Card className="mb-6">
        <CardHeader>
          <CardTitle>Search & Filter</CardTitle>
          <CardDescription>Search POIs by name, ID, address, or category</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex items-center space-x-2">
            <Input
              placeholder="Search POIs..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="flex-1"
            />
            {searchTerm && (
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setSearchTerm("")}
              >
                Clear
              </Button>
            )}
          </div>
          {searchTerm && (
            <p className="text-sm text-muted-foreground mt-2">
              Showing {filteredFeatures.length} of {geojson.features.length} POIs
            </p>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>POI Features</CardTitle>
          <CardDescription>
            Click Edit to modify POI properties. Changes are saved locally until you click &quot;Save Changes&quot;.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {featuresToShow.length === 0 ? (
            <Alert>
              <AlertDescription>
                No POIs found matching your search.
              </AlertDescription>
            </Alert>
          ) : (
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>ID</TableHead>
                    <TableHead>Coordinates</TableHead>
                    {propertyKeys.slice(0, 5).map((key) => (
                      <TableHead key={key}>{key}</TableHead>
                    ))}
                    <TableHead className="w-[100px]">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {featuresToShow.map((feature, index) => {
                    const isEditing = editingIndex === index
                    const [lng, lat] = feature.geometry.coordinates

                    return (
                      <TableRow key={feature.id || index}>
                        <TableCell className="font-mono text-xs">
                          {feature.id?.toString() || `#${index}`}
                        </TableCell>
                        <TableCell className="font-mono text-xs">
                          <div className="flex items-center gap-1">
                            <MapPin className="h-3 w-3" />
                            {lat.toFixed(6)}, {lng.toFixed(6)}
                          </div>
                        </TableCell>
                        {propertyKeys.slice(0, 5).map((key) => (
                          <TableCell key={key}>
                            {isEditing ? (
                              <Input
                                value={editedProperties?.[key]?.toString() || ""}
                                onChange={(e) => updateProperty(key, e.target.value)}
                                className="h-8 text-xs"
                                placeholder={key}
                              />
                            ) : (
                              <span className="text-sm">
                                {getPropertyValue(feature, key) || (
                                  <span className="text-muted-foreground">—</span>
                                )}
                              </span>
                            )}
                          </TableCell>
                        ))}
                        <TableCell>
                          {isEditing ? (
                            <div className="flex items-center gap-1">
                              <Button
                                size="sm"
                                variant="ghost"
                                onClick={saveEdit}
                                className="h-7 w-7 p-0"
                              >
                                <Check className="h-4 w-4 text-green-500" />
                              </Button>
                              <Button
                                size="sm"
                                variant="ghost"
                                onClick={cancelEditing}
                                className="h-7 w-7 p-0"
                              >
                                <X className="h-4 w-4 text-red-500" />
                              </Button>
                            </div>
                          ) : (
                            <Button
                              size="sm"
                              variant="ghost"
                              onClick={() => startEditing(index)}
                              className="h-7 w-7 p-0"
                            >
                              <Edit2 className="h-4 w-4" />
                            </Button>
                          )}
                        </TableCell>
                      </TableRow>
                    )
                  })}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      {propertyKeys.length > 5 && (
        <Card className="mt-6">
          <CardHeader>
            <CardTitle>All Properties</CardTitle>
            <CardDescription>
              Additional properties available in this collection
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap gap-2">
              {propertyKeys.slice(5).map((key) => (
                <Badge key={key} variant="secondary">
                  {key}
                </Badge>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </MainLayout>
  )
}
