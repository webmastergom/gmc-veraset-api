"use client"

import { useState, useEffect, useRef } from "react"
import { useRouter } from "next/navigation"
import { MainLayout } from "@/components/layout/main-layout"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import Link from "next/link"
import { ArrowLeft, Loader2, Upload, File, X } from "lucide-react"
import { useToast } from "@/hooks/use-toast"
import { type VerasetJobConfig } from "@/lib/veraset-client"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"

export default function NewJobPage() {
  const router = useRouter()
  const { toast } = useToast()
  const [loading, setLoading] = useState(false)
  const [collections, setCollections] = useState<any[]>([])
  const [usage, setUsage] = useState<any>(null)
  const [showConfirmDialog, setShowConfirmDialog] = useState(false)
  const [poiSource, setPoiSource] = useState<"collection" | "upload">("collection")
  const [uploadedFile, setUploadedFile] = useState<File | null>(null)
  const [uploadedGeojson, setUploadedGeojson] = useState<any>(null)
  const [uploadPreview, setUploadPreview] = useState<{ poiCount: number; isValid: boolean } | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [formData, setFormData] = useState<Partial<VerasetJobConfig>>({
    name: "",
    type: "pings",
    poiCollection: "",
    dateRange: {
      from: "",
      to: "",
    },
    radius: 10,
    schema: "BASIC",
  })

  useEffect(() => {
    // Fetch POI collections
    fetch("/api/pois/collections", {
      credentials: 'include',
    })
      .then(res => res.json())
      .then(data => setCollections(data))
      .catch(err => console.error("Error fetching collections:", err))
    
    // Fetch usage
    fetch("/api/usage", {
      credentials: 'include',
    })
      .then(res => res.json())
      .then(data => setUsage(data))
      .catch(err => console.error("Error fetching usage:", err))
  }, [])

  const handleFileSelect = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!e.target.files || !e.target.files[0]) return

    const selectedFile = e.target.files[0]

    // Validate file type
    if (!selectedFile.name.endsWith('.geojson') && !selectedFile.name.endsWith('.json')) {
      toast({
        title: "Invalid File Type",
        description: "Please upload a GeoJSON file (.geojson or .json)",
        variant: "destructive",
      })
      return
    }

    // Validate file size (50MB max)
    if (selectedFile.size > 50 * 1024 * 1024) {
      toast({
        title: "File Too Large",
        description: "Maximum file size is 50MB",
        variant: "destructive",
      })
      return
    }

    setUploadedFile(selectedFile)

    try {
      const text = await selectedFile.text()
      const geojson = JSON.parse(text)

      // Validate GeoJSON structure
      if (!geojson.type || geojson.type !== 'FeatureCollection') {
        toast({
          title: "Invalid GeoJSON",
          description: "File must be a FeatureCollection",
          variant: "destructive",
        })
        setUploadPreview({ poiCount: 0, isValid: false })
        setUploadedGeojson(null)
        return
      }

      // Count valid Point features
      const validPoints = (geojson.features || []).filter((f: any) => {
        return (
          f.geometry &&
          f.geometry.type === 'Point' &&
          Array.isArray(f.geometry.coordinates) &&
          f.geometry.coordinates.length >= 2 &&
          typeof f.geometry.coordinates[0] === 'number' &&
          typeof f.geometry.coordinates[1] === 'number' &&
          !isNaN(f.geometry.coordinates[0]) &&
          !isNaN(f.geometry.coordinates[1]) &&
          f.geometry.coordinates[0] >= -180 &&
          f.geometry.coordinates[0] <= 180 &&
          f.geometry.coordinates[1] >= -90 &&
          f.geometry.coordinates[1] <= 90
        )
      }).length

      setUploadPreview({
        poiCount: validPoints,
        isValid: validPoints > 0,
      })
      setUploadedGeojson(geojson)

      if (validPoints === 0) {
        toast({
          title: "No Valid POIs",
          description: "The file doesn't contain any valid Point geometries",
          variant: "destructive",
        })
      }
    } catch (error) {
      toast({
        title: "Error Reading File",
        description: "Failed to parse GeoJSON file. Please check the file format.",
        variant: "destructive",
      })
      setUploadPreview({ poiCount: 0, isValid: false })
      setUploadedGeojson(null)
    }
  }

  const removeUploadedFile = () => {
    setUploadedFile(null)
    setUploadedGeojson(null)
    setUploadPreview(null)
    if (fileInputRef.current) {
      fileInputRef.current.value = ''
    }
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    
    // Validation
    if (!formData.name || !formData.dateRange?.from || !formData.dateRange?.to) {
      toast({
        title: "Validation Error",
        description: "Please fill in all required fields",
        variant: "destructive",
      })
      return
    }

    // Validate POI source
    if (poiSource === "collection" && !formData.poiCollection) {
      toast({
        title: "Validation Error",
        description: "Please select a POI collection or upload a file",
        variant: "destructive",
      })
      return
    }

    if (poiSource === "upload" && (!uploadedGeojson || !uploadPreview?.isValid)) {
      toast({
        title: "Validation Error",
        description: "Please upload a valid GeoJSON file",
        variant: "destructive",
      })
      return
    }

    // Validate date range (max 31 days)
    const from = new Date(formData.dateRange.from)
    const to = new Date(formData.dateRange.to)
    const daysDiff = Math.ceil((to.getTime() - from.getTime()) / (1000 * 60 * 60 * 24))
    
    if (daysDiff > 31) {
      toast({
        title: "Validation Error",
        description: "Date range cannot exceed 31 days",
        variant: "destructive",
      })
      return
    }

    if (daysDiff < 1) {
      toast({
        title: "Validation Error",
        description: "End date must be after start date",
        variant: "destructive",
      })
      return
    }

    // Check usage first
    if (!usage) {
      const usageRes = await fetch('/api/usage', {
        credentials: 'include',
      })
      const usageData = await usageRes.json()
      setUsage(usageData)
      
      if (usageData.remaining <= 0) {
        toast({
          title: "API Limit Reached",
          description: "You've used all 200 API calls this month. Limit resets on the 1st of next month.",
          variant: "destructive"
        })
        return
      }
      
      if (usageData.remaining <= 10) {
        setShowConfirmDialog(true)
        return
      }
    } else {
      if (usage.remaining <= 0) {
        toast({
          title: "API Limit Reached",
          description: "You've used all 200 API calls this month. Limit resets on the 1st of next month.",
          variant: "destructive"
        })
        return
      }
      
      if (usage.remaining <= 10) {
        setShowConfirmDialog(true)
        return
      }
    }

    // Proceed with job creation
    await submitJob()
  }

  const submitJob = async () => {
    setShowConfirmDialog(false)
    setLoading(true)

    try {
      let geojson: any
      let poiCount: number

      if (poiSource === "upload" && uploadedGeojson) {
        // Use uploaded file
        geojson = uploadedGeojson
        poiCount = uploadPreview?.poiCount || 0
      } else {
        // Fetch POIs from collection
        const collectionResponse = await fetch(`/api/pois/collections`)
        const collections = await collectionResponse.json()
        const collection = collections.find((c: any) => c.id === formData.poiCollection)
        
        if (!collection) {
          toast({
            title: "Error",
            description: "POI collection not found",
            variant: "destructive",
          })
          setLoading(false)
          return
        }

        // Fetch GeoJSON from S3
        const geojsonResponse = await fetch(`/api/pois/collections/${collection.id}/geojson`)
        if (!geojsonResponse.ok) {
          toast({
            title: "Error",
            description: "Failed to load POI collection data",
            variant: "destructive",
          })
          setLoading(false)
          return
        }
        
        geojson = await geojsonResponse.json()
        poiCount = collection.poiCount || 0
      }
      
      // Validate and filter POIs - only include features with valid Point geometry
      const totalFeatures = geojson.features?.length || 0
      const pois = (geojson.features || [])
        .filter((f: any) => {
          // Only process Point geometries with valid coordinates
          return (
            f.geometry &&
            f.geometry.type === 'Point' &&
            Array.isArray(f.geometry.coordinates) &&
            f.geometry.coordinates.length >= 2 &&
            typeof f.geometry.coordinates[0] === 'number' &&
            typeof f.geometry.coordinates[1] === 'number' &&
            !isNaN(f.geometry.coordinates[0]) &&
            !isNaN(f.geometry.coordinates[1]) &&
            f.geometry.coordinates[0] >= -180 &&
            f.geometry.coordinates[0] <= 180 &&
            f.geometry.coordinates[1] >= -90 &&
            f.geometry.coordinates[1] <= 90
          )
        })
        .map((f: any, index: number) => {
          // Extract POI ID from GeoJSON feature
          // Priority: f.id > properties.id > properties.poi_id > properties.identifier > auto-generated
          const poiId = 
            f.id || 
            f.properties?.id || 
            f.properties?.poi_id || 
            f.properties?.identifier ||
            `geo_radius_${index}`; // Fallback to auto-generated name
          
          // Extract POI name separately (for display purposes)
          // Priority: properties.name > properties.poi_name > poiId (if not auto-generated)
          const poiName = 
            f.properties?.name || 
            f.properties?.poi_name ||
            (poiId && !String(poiId).startsWith('geo_radius_') ? String(poiId) : null);
          
          return {
            latitude: f.geometry.coordinates[1],
            longitude: f.geometry.coordinates[0],
            radius: formData.radius || 10,
            poiId: String(poiId), // Ensure it's a string
            poiName: poiName ? String(poiName) : null, // Human-readable name
            originalIndex: index, // Keep track of original order
            properties: f.properties || {}, // Keep all properties for reference
          }
        })
      
      const invalidFeatures = totalFeatures - pois.length
      const actualValidPoiCount = pois.length // Use actual count after filtering
      
      // Create mapping from Veraset auto-generated IDs to original GeoJSON IDs
      // Veraset generates IDs as geo_radius_0, geo_radius_1, etc. based on array index
      const poiMapping: Record<string, string> = {}
      const poiNames: Record<string, string> = {} // Maps Veraset IDs to human-readable names
      const poiIdSources: Record<string, string> = {} // Track where each ID came from
      
      pois.forEach((poi: { poiId: string; poiName: string | null; properties?: Record<string, unknown> }, index: number) => {
        const verasetId = `geo_radius_${index}`
        poiMapping[verasetId] = poi.poiId
        
        // Store human-readable name if available
        if (poi.poiName) {
          poiNames[verasetId] = poi.poiName
        }
        
        // Track the source of the POI ID for debugging
        const source = poi.properties?.id ? 'properties.id' :
                      poi.properties?.poi_id ? 'properties.poi_id' :
                      poi.properties?.identifier ? 'properties.identifier' :
                      'auto-generated';
        poiIdSources[poi.poiId] = source;
      })
      
      // Log detailed information
      console.log(`📊 Job Creation - POI Processing:`)
      console.log(`   - Total features in GeoJSON: ${totalFeatures}`)
      console.log(`   - Valid POIs after filtering: ${actualValidPoiCount}`)
      console.log(`   - Invalid features filtered: ${invalidFeatures}`)
      
      // Log POI ID extraction details
      const samplePois = pois.slice(0, 5);
      console.log(`   - Sample POI IDs extracted:`)
      samplePois.forEach((poi: { poiId: string; poiName: string | null }, idx: number) => {
        const verasetId = `geo_radius_${idx}`;
        const nameInfo = poi.poiName ? ` (name: "${poi.poiName}")` : '';
        console.log(`     ${verasetId} -> "${poi.poiId}"${nameInfo} (from ${poiIdSources[poi.poiId]})`)
      })
      if (pois.length > 5) {
        console.log(`     ... and ${pois.length - 5} more`)
      }
      
      // Log how many POIs have names
      const poisWithNames = pois.filter((p: { poiName: string | null }) => p.poiName).length;
      if (poisWithNames > 0) {
        console.log(`   - ${poisWithNames} POIs have human-readable names from GeoJSON`)
      }
      
      // Check if all POIs have meaningful IDs or if they're all auto-generated
      const meaningfulIds = pois.filter((p: { poiId: string }) => !p.poiId.startsWith('geo_radius_')).length;
      if (meaningfulIds === 0 && pois.length > 0) {
        console.warn(`   ⚠️ All POI IDs are auto-generated. Consider adding 'id' or 'properties.id' to your GeoJSON features.`)
      } else {
        console.log(`   - ${meaningfulIds} POIs have meaningful IDs from GeoJSON`)
      }
      if (poiSource === "collection") {
        console.log(`   - Collection expected count: ${poiCount}`)
        if (actualValidPoiCount !== poiCount) {
          console.warn(`   ⚠️ Mismatch: Collection says ${poiCount} but found ${actualValidPoiCount} valid POIs`)
        }
      }
      
      if (invalidFeatures > 0) {
        console.warn(`⚠️ Filtered out ${invalidFeatures} invalid features from ${totalFeatures} total`)
      }
      
      if (pois.length === 0) {
        toast({
          title: "Error",
          description: `POI collection is empty or contains no valid Point geometries. Found ${totalFeatures} features but none were valid.`,
          variant: "destructive",
        })
        setLoading(false)
        return
      }
      
      if (pois.length !== totalFeatures && invalidFeatures > 0) {
        toast({
          title: "Warning",
          description: `Only ${pois.length} of ${totalFeatures} POIs are valid. ${invalidFeatures} features were filtered out.`,
          variant: "default",
        })
      }
      
      // Use actual valid count, not the collection count
      poiCount = actualValidPoiCount

      if (pois.length > 25000) {
        toast({
          title: "Validation Error",
          description: "POI count cannot exceed 25,000",
          variant: "destructive",
        })
        setLoading(false)
        return
      }

      // Check usage one more time before creating
      const usageCheck = await fetch('/api/usage', {
        credentials: 'include',
      })
      const currentUsage = await usageCheck.json()
      
      if (currentUsage.remaining <= 0) {
        toast({
          title: "API Limit Reached",
          description: "You've used all 200 API calls this month. Limit resets on the 1st of next month.",
          variant: "destructive"
        })
        setLoading(false)
        return
      }

      // Create job via API route (which handles Veraset API call and S3 storage)
      const createResponse = await fetch("/api/jobs", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        credentials: 'include',
        body: JSON.stringify({
          name: formData.name,
          type: formData.type,
          poiCollectionId: formData.poiCollection,
          poiCount: pois.length,
          dateRange: formData.dateRange,
          radius: formData.radius,
          schema: formData.schema,
          verasetConfig: (() => {
            // Split POIs into enriched (with place_key) and non-enriched (geo_radius)
            const enrichedPois = pois.filter((poi: any) => poi.properties?.place_key);
            const geoRadiusPois = pois.filter((poi: any) => !poi.properties?.place_key);

            const config: Record<string, any> = {
              type: formData.type,
              date_range: {
                from_date: formData.dateRange!.from,
                to_date: formData.dateRange!.to,
              },
              schema: formData.schema,
            };

            // Add place_key array for enriched POIs
            if (enrichedPois.length > 0) {
              config.place_key = enrichedPois.map((poi: any) => ({
                poi_id: poi.poiId,
                placekey: poi.properties.place_key,
              }));
            }

            // Add geo_radius array for non-enriched POIs
            if (geoRadiusPois.length > 0) {
              config.geo_radius = geoRadiusPois.map((poi: { latitude: number; longitude: number; radius: number; poiId: string }) => ({
                poi_id: poi.poiId,
                latitude: poi.latitude,
                longitude: poi.longitude,
                distance_in_meters: poi.radius,
              }));
            }

            console.log(`📊 Veraset Config: ${enrichedPois.length} place_key + ${geoRadiusPois.length} geo_radius`);

            return config;
          })(),
          poiMapping: poiMapping, // Include mapping for later reference
          poiNames: poiNames, // Include human-readable names for display
        }),
      })

      if (!createResponse.ok) {
        const error = await createResponse.json()
        if (createResponse.status === 429) {
          toast({
            title: "API Limit Reached",
            description: error.error || "Monthly API limit reached (200 calls). Limit resets on the 1st of next month.",
            variant: "destructive"
          })
          setLoading(false)
          return
        }
        throw new Error(error.error || "Failed to create job")
      }

      const job = await createResponse.json()
      
      toast({
        title: "Job Created",
        description: `Job ${job.jobId} has been created successfully`,
      })

      router.push(`/jobs/${job.jobId}`)
    } catch (error) {
      toast({
        title: "Error",
        description: error instanceof Error ? error.message : "Failed to create job",
        variant: "destructive",
      })
    } finally {
      setLoading(false)
    }
  }

  return (
    <MainLayout>
      <div className="mb-6">
        <Link href="/jobs">
          <Button variant="ghost" className="mb-4">
            <ArrowLeft className="mr-2 h-4 w-4" />
            Back to Jobs
          </Button>
        </Link>
        <h1 className="text-3xl font-bold text-white">Create New Job</h1>
        <p className="text-gray-500 mt-2">
          Configure a new Veraset API job
        </p>
      </div>

      <Card className="max-w-2xl">
        <CardHeader>
          <CardTitle>Job Configuration</CardTitle>
          <CardDescription>
            Fill in the details to create a new Veraset job. Maximum 25,000 POIs and 31 days date range.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-6">
            <div className="space-y-2">
              <Label htmlFor="name">Job Name *</Label>
              <Input
                id="name"
                placeholder="e.g., Spain Nicotine Full - Jan 2026"
                value={formData.name}
                onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                required
              />
            </div>

            <div className="space-y-2">
              <Label>Job Type *</Label>
              <Select
                value={formData.type}
                onValueChange={(value: any) => setFormData({ ...formData, type: value })}
              >
                <SelectTrigger id="type" aria-label="Job Type">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="pings">Pings</SelectItem>
                  <SelectItem value="aggregate">Aggregate</SelectItem>
                  <SelectItem value="devices">Devices</SelectItem>
                  <SelectItem value="cohort">Cohort</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-4">
              <div className="space-y-2">
                <Label>POI Source *</Label>
                <div className="flex gap-4">
                  <label className="flex items-center space-x-2 cursor-pointer">
                    <input
                      type="radio"
                      name="poiSource"
                      value="collection"
                      checked={poiSource === "collection"}
                      onChange={(e) => {
                        setPoiSource("collection")
                        setUploadedFile(null)
                        setUploadedGeojson(null)
                        setUploadPreview(null)
                      }}
                      className="w-4 h-4"
                    />
                    <span>Use Existing Collection</span>
                  </label>
                  <label className="flex items-center space-x-2 cursor-pointer">
                    <input
                      type="radio"
                      name="poiSource"
                      value="upload"
                      checked={poiSource === "upload"}
                      onChange={(e) => {
                        setPoiSource("upload")
                        setFormData({ ...formData, poiCollection: "" })
                      }}
                      className="w-4 h-4"
                    />
                    <span>Upload GeoJSON File</span>
                  </label>
                </div>
              </div>

              {poiSource === "collection" ? (
                <div className="space-y-2">
                  <Label>POI Collection *</Label>
                  <Select
                    value={formData.poiCollection}
                    onValueChange={(value) => setFormData({ ...formData, poiCollection: value })}
                  >
                    <SelectTrigger id="poiCollection" aria-label="POI Collection">
                      <SelectValue placeholder="Select a POI collection" />
                    </SelectTrigger>
                    <SelectContent>
                      {collections.length === 0 ? (
                        <div className="px-2 py-1.5 text-sm text-muted-foreground">No collections available</div>
                      ) : (
                        collections.map((col) => (
                          <SelectItem key={col.id} value={col.id}>
                            {col.name} ({col.poiCount || 0} POIs{col.enrichedCount ? `, ${col.enrichedCount} enriched` : ''})
                          </SelectItem>
                        ))
                      )}
                    </SelectContent>
                  </Select>
                </div>
              ) : (
                <div className="space-y-2">
                  <Label>GeoJSON File *</Label>
                  <div className="border-2 border-dashed rounded-lg p-6">
                    {uploadedFile ? (
                      <div className="space-y-3">
                        <div className="flex items-center justify-between">
                          <div className="flex items-center space-x-3">
                            <File className="h-6 w-6 text-primary" />
                            <div>
                              <p className="font-medium">{uploadedFile.name}</p>
                              <p className="text-sm text-muted-foreground">
                                {(uploadedFile.size / 1024 / 1024).toFixed(2)} MB
                              </p>
                            </div>
                          </div>
                          <Button
                            type="button"
                            variant="ghost"
                            size="icon"
                            onClick={removeUploadedFile}
                          >
                            <X className="h-4 w-4" />
                          </Button>
                        </div>
                        {uploadPreview && (
                          <div className={`p-2 rounded text-sm ${
                            uploadPreview.isValid 
                              ? 'bg-green-500/10 text-green-400' 
                              : 'bg-red-500/10 text-red-400'
                          }`}>
                            {uploadPreview.isValid
                              ? `✓ ${uploadPreview.poiCount.toLocaleString()} valid POIs found`
                              : '✗ No valid POIs found'}
                          </div>
                        )}
                      </div>
                    ) : (
                      <div className="text-center space-y-3">
                        <Upload className="h-8 w-8 mx-auto text-muted-foreground" />
                        <div>
                          <p className="text-sm text-muted-foreground mb-3">
                            Click to select a GeoJSON file
                          </p>
                          <Button
                            type="button"
                            variant="outline"
                            onClick={() => fileInputRef.current?.click()}
                          >
                            Select File
                          </Button>
                          <input
                            ref={fileInputRef}
                            type="file"
                            accept=".geojson,.json"
                            onChange={handleFileSelect}
                            className="hidden"
                          />
                        </div>
                      </div>
                    )}
                  </div>
                  <p className="text-sm text-muted-foreground">
                    Only FeatureCollection GeoJSON files with Point geometries are supported. Max 50MB.
                  </p>
                </div>
              )}
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="from">Start Date *</Label>
                <Input
                  id="from"
                  type="date"
                  value={formData.dateRange?.from}
                  onChange={(e) =>
                    setFormData({
                      ...formData,
                      dateRange: { ...formData.dateRange!, from: e.target.value },
                    })
                  }
                  required
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="to">End Date *</Label>
                <Input
                  id="to"
                  type="date"
                  value={formData.dateRange?.to}
                  onChange={(e) =>
                    setFormData({
                      ...formData,
                      dateRange: { ...formData.dateRange!, to: e.target.value },
                    })
                  }
                  required
                />
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="radius">Radius (meters) *</Label>
              <Input
                id="radius"
                type="number"
                min="1"
                max="1000"
                value={formData.radius}
                onChange={(e) => setFormData({ ...formData, radius: parseInt(e.target.value) || 10 })}
                required
              />
              <p className="text-sm text-muted-foreground">
                Default: 10 meters. Maximum: 1000 meters.
              </p>
            </div>

            <div className="space-y-2">
              <Label>Schema *</Label>
              <Select
                value={formData.schema}
                onValueChange={(value: "BASIC" | "FULL") => setFormData({ ...formData, schema: value })}
              >
                <SelectTrigger id="schema" aria-label="Schema">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="BASIC">BASIC (Recommended)</SelectItem>
                  <SelectItem value="FULL">FULL</SelectItem>
                </SelectContent>
              </Select>
              <p className="text-sm text-muted-foreground">
                BASIC schema includes: ad_id, utc_timestamp, latitude, longitude, poi_ids
              </p>
            </div>

            <div className="flex justify-end space-x-4">
              <Link href="/jobs">
                <Button type="button" variant="outline">
                  Cancel
                </Button>
              </Link>
              <Button type="submit" disabled={loading}>
                {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                Create Job
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>

      {/* Confirmation Dialog for Low Usage */}
      <AlertDialog open={showConfirmDialog} onOpenChange={setShowConfirmDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Low API Calls Warning</AlertDialogTitle>
            <AlertDialogDescription>
              <div className="space-y-2">
                <p>
                  You only have <strong>{usage?.remaining || 0} API calls</strong> remaining this month.
                </p>
                <p className="text-sm text-muted-foreground">
                  This job will use 1 call, leaving you with {usage ? usage.remaining - 1 : 0} calls.
                </p>
                <p className="text-sm text-yellow-600 font-medium">
                  ⚠️ The limit resets on the 1st of next month.
                </p>
              </div>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={submitJob}>
              Create Job ({usage ? usage.remaining - 1 : 0} calls left)
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </MainLayout>
  )
}
