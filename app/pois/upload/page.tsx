"use client"

import { useState, useRef, DragEvent } from "react"
import { useRouter } from "next/navigation"
import Papa from "papaparse"
import { MainLayout } from "@/components/layout/main-layout"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import Link from "next/link"
import { ArrowLeft, Upload, File, X, Loader2 } from "lucide-react"
import { useToast } from "@/hooks/use-toast"

const ACCEPTED_EXTENSIONS = [".geojson", ".json", ".csv"]
const ACCEPT_ATTR = ".geojson,.json,.csv"

interface GeoJSONFeature {
  type: "Feature"
  id?: string | number
  geometry: { type: "Point"; coordinates: [number, number] }
  properties: Record<string, string | number>
}

/** Parse CSV to GeoJSON FeatureCollection. Expects columns: lat/latitude, lon/longitude/lng, optionally name, id, address, etc. */
function csvToGeoJSON(text: string): { type: "FeatureCollection"; features: GeoJSONFeature[] } | null {
  const result = Papa.parse<string[]>(text, { skipEmptyLines: true })
  const rows = result.data
  if (!rows || rows.length < 2) return null

  const headers = rows[0].map((h) => String(h || "").trim().toLowerCase())
  const latCol = headers.findIndex((h) => ["lat", "latitude", "y"].includes(h))
  const lonCol = headers.findIndex((h) => ["lon", "lng", "longitude", "long", "x"].includes(h))
  if (latCol < 0 || lonCol < 0) return null

  const nameCol = headers.findIndex((h) => ["name", "poi_name", "nombre", "title"].includes(h))
  const idCol = headers.findIndex((h) => ["id", "poi_id", "identifier"].includes(h))

  const features: GeoJSONFeature[] = []
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i]
    const lat = parseFloat(String(row[latCol] ?? "").trim())
    const lon = parseFloat(String(row[lonCol] ?? "").trim())
    if (isNaN(lat) || isNaN(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180) continue

    const props: Record<string, string | number> = {}
    headers.forEach((h, j) => {
      if (j !== latCol && j !== lonCol && row[j] !== undefined && row[j] !== "") {
        const val = String(row[j]).trim()
        const num = parseFloat(val)
        props[h] = isNaN(num) || val !== String(num) ? val : num
      }
    })
    const name = nameCol >= 0 ? String(row[nameCol] ?? "").trim() : undefined
    const id = idCol >= 0 ? String(row[idCol] ?? "").trim() : `poi-${i}`

    if (name) props.name = name
    if (id) props.id = id

    features.push({
      type: "Feature",
      id: id || `poi-${i}`,
      geometry: { type: "Point", coordinates: [lon, lat] },
      properties: props,
    })
  }
  return features.length > 0 ? { type: "FeatureCollection", features } : null
}

export default function POIUploadPage() {
  const router = useRouter()
  const { toast } = useToast()
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [file, setFile] = useState<File | null>(null)
  const [collectionName, setCollectionName] = useState("")
  const [description, setDescription] = useState("")
  const [loading, setLoading] = useState(false)
  const [dragActive, setDragActive] = useState(false)
  const [preview, setPreview] = useState<{ poiCount: number; isValid: boolean } | null>(null)

  const handleDrag = (e: DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true)
    } else if (e.type === "dragleave") {
      setDragActive(false)
    }
  }

  const handleDrop = (e: DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    setDragActive(false)

    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      handleFile(e.dataTransfer.files[0])
    }
  }

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      handleFile(e.target.files[0])
    }
  }

  const isValidPoint = (f: any) =>
    f.geometry &&
    f.geometry.type === "Point" &&
    Array.isArray(f.geometry.coordinates) &&
    f.geometry.coordinates.length >= 2 &&
    typeof f.geometry.coordinates[0] === "number" &&
    typeof f.geometry.coordinates[1] === "number" &&
    !isNaN(f.geometry.coordinates[0]) &&
    !isNaN(f.geometry.coordinates[1]) &&
    f.geometry.coordinates[0] >= -180 &&
    f.geometry.coordinates[0] <= 180 &&
    f.geometry.coordinates[1] >= -90 &&
    f.geometry.coordinates[1] <= 90

  const setNameFromFile = (filename: string) => {
    if (collectionName) return
    const nameFromFile = filename
      .replace(/\.(geojson|json|csv)$/i, "")
      .replace(/[^a-z0-9]+/gi, " ")
      .trim()
      .split(" ")
      .map((w: string) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
      .join(" ")
    setCollectionName(nameFromFile)
  }

  const handleFile = async (selectedFile: File) => {
    const ext = "." + selectedFile.name.split(".").pop()?.toLowerCase()
    if (!ACCEPTED_EXTENSIONS.includes(ext)) {
      toast({
        title: "Invalid File Type",
        description: "Please upload a GeoJSON (.geojson, .json) or CSV (.csv) file",
        variant: "destructive",
      })
      return
    }

    if (selectedFile.size > 50 * 1024 * 1024) {
      toast({
        title: "File Too Large",
        description: "Maximum file size is 50MB",
        variant: "destructive",
      })
      return
    }

    setFile(selectedFile)

    try {
      const text = await selectedFile.text()
      let geojson: any

      if (ext === ".csv") {
        geojson = csvToGeoJSON(text)
        if (!geojson) {
          toast({
            title: "Invalid CSV",
            description: "CSV must have headers with lat/latitude and lon/longitude/lng columns",
            variant: "destructive",
          })
          setPreview({ poiCount: 0, isValid: false })
          return
        }
      } else {
        geojson = JSON.parse(text)
        if (!geojson.type || geojson.type !== "FeatureCollection") {
          toast({
            title: "Invalid GeoJSON",
            description: "File must be a FeatureCollection",
            variant: "destructive",
          })
          setPreview({ poiCount: 0, isValid: false })
          return
        }
      }

      const validPoints = (geojson.features || []).filter(isValidPoint).length
      setPreview({ poiCount: validPoints, isValid: validPoints > 0 })

      if (validPoints === 0) {
        toast({
          title: "No Valid POIs",
          description: ext === ".csv"
            ? "CSV must have numeric lat/lon columns. Check column names."
            : "The file doesn't contain any valid Point geometries",
          variant: "destructive",
        })
      } else {
        setNameFromFile(selectedFile.name)
      }
    } catch (error) {
      toast({
        title: "Error Reading File",
        description: "Failed to parse file. Please check the format.",
        variant: "destructive",
      })
      setPreview({ poiCount: 0, isValid: false })
    }
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()

    if (!file) {
      toast({
        title: "No File Selected",
        description: "Please select a GeoJSON or CSV file to upload",
        variant: "destructive",
      })
      return
    }

    if (!collectionName.trim()) {
      toast({
        title: "Collection Name Required",
        description: "Please enter a name for the POI collection",
        variant: "destructive",
      })
      return
    }

    if (!preview?.isValid) {
      toast({
        title: "Invalid File",
        description: "The file doesn't contain valid POI data",
        variant: "destructive",
      })
      return
    }

    setLoading(true)

    try {
      const text = await file.text()
      const ext = "." + file.name.split(".").pop()?.toLowerCase()
      const geojson =
        ext === ".csv"
          ? csvToGeoJSON(text)
          : JSON.parse(text)

      if (!geojson) {
        throw new Error("Invalid CSV format")
      }

      const collectionId = collectionName.trim().toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '')
      const geojsonStr = JSON.stringify(geojson)
      const geojsonSizeMB = geojsonStr.length / (1024 * 1024)

      // For large files (>3MB), use presigned URL to bypass Vercel body limit
      if (geojsonSizeMB > 3) {
        // Step 1: Get presigned S3 upload URL
        const urlRes = await fetch('/api/pois/upload-url', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'include',
          body: JSON.stringify({ collectionId }),
        })

        if (!urlRes.ok) {
          const err = await urlRes.json()
          throw new Error(err.error || 'Failed to get upload URL')
        }

        const { uploadUrl } = await urlRes.json()

        // Step 2: Upload GeoJSON directly to S3 (no Vercel limit)
        const s3Res = await fetch(uploadUrl, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: geojsonStr,
        })

        if (!s3Res.ok) {
          throw new Error(`S3 upload failed: ${s3Res.status} ${s3Res.statusText}`)
        }

        // Step 3: Create collection metadata (small payload, no GeoJSON)
        const response = await fetch('/api/pois/collections', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'include',
          body: JSON.stringify({
            id: collectionId,
            name: collectionName.trim(),
            description: description.trim(),
            poiCount: preview.poiCount,
            // No geojson — already uploaded directly to S3
          }),
        })

        if (!response.ok) {
          const error = await response.json()
          throw new Error(error.error || 'Failed to create collection')
        }

        const collection = await response.json()

        toast({
          title: "Collection Created",
          description: `Successfully created "${collection.name}" with ${preview.poiCount.toLocaleString()} POIs (${geojsonSizeMB.toFixed(1)}MB uploaded)`,
        })
      } else {
        // Small files: send everything in one request (original flow)
        const response = await fetch('/api/pois/collections', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'include',
          body: JSON.stringify({
            name: collectionName.trim(),
            description: description.trim(),
            geojson: geojson,
            poiCount: preview.poiCount,
          }),
        })

        if (!response.ok) {
          const error = await response.json()
          throw new Error(error.error || 'Failed to create collection')
        }

        const collection = await response.json()

        toast({
          title: "Collection Created",
          description: `Successfully created "${collection.name}" with ${preview.poiCount.toLocaleString()} POIs`,
        })
      }

      // Redirect to POIs page
      router.push('/pois')
    } catch (error: any) {
      toast({
        title: "Upload Failed",
        description: error.message || "Failed to upload POI collection",
        variant: "destructive",
      })
    } finally {
      setLoading(false)
    }
  }

  const removeFile = () => {
    setFile(null)
    setPreview(null)
    if (fileInputRef.current) {
      fileInputRef.current.value = ''
    }
  }

  return (
    <MainLayout>
      <div className="mb-6">
        <Link href="/pois">
          <Button variant="ghost" className="mb-4">
            <ArrowLeft className="mr-2 h-4 w-4" />
            Back to POIs
          </Button>
        </Link>
        <h1 className="text-3xl font-bold text-white">Upload POIs</h1>
        <p className="text-gray-500 mt-2">
          Upload GeoJSON or CSV files containing Points of Interest
        </p>
      </div>

      <Card className="max-w-2xl">
        <CardHeader>
          <CardTitle>File Upload</CardTitle>
          <CardDescription>
            Upload a GeoJSON (.geojson, .json) or CSV file with lat/lon columns. Maximum file size: 50MB.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-6">
            {/* File Upload Area */}
            <div className="space-y-2">
              <Label>GeoJSON or CSV File *</Label>
              <div
                className={`border-2 border-dashed rounded-lg p-8 text-center transition-colors ${
                  dragActive
                    ? 'border-primary bg-primary/5'
                    : 'border-gray-600 hover:border-gray-500'
                }`}
                onDragEnter={handleDrag}
                onDragLeave={handleDrag}
                onDragOver={handleDrag}
                onDrop={handleDrop}
              >
                {file ? (
                  <div className="space-y-4">
                    <div className="flex items-center justify-center space-x-3">
                      <File className="h-8 w-8 text-primary" />
                      <div className="text-left">
                        <p className="font-medium">{file.name}</p>
                        <p className="text-sm text-muted-foreground">
                          {(file.size / 1024 / 1024).toFixed(2)} MB
                        </p>
                      </div>
                      <Button
                        type="button"
                        variant="ghost"
                        size="icon"
                        onClick={removeFile}
                        className="ml-2"
                      >
                        <X className="h-4 w-4" />
                      </Button>
                    </div>
                    {preview && (
                      <div className={`p-3 rounded ${preview.isValid ? 'bg-green-500/10 text-green-400' : 'bg-red-500/10 text-red-400'}`}>
                        <p className="text-sm font-medium">
                          {preview.isValid
                            ? `✓ ${preview.poiCount.toLocaleString()} valid POIs found`
                            : '✗ No valid POIs found'}
                        </p>
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="space-y-4">
                    <Upload className="h-12 w-12 mx-auto text-muted-foreground" />
                    <div>
                      <p className="text-muted-foreground mb-4">
                        Drag and drop a GeoJSON or CSV file here, or click to browse
                      </p>
                      <Button
                        type="button"
                        variant="outline"
                        onClick={() => fileInputRef.current?.click()}
                      >
                        Select File
                      </Button>
                    </div>
                  </div>
                )}
                <input
                  ref={fileInputRef}
                  type="file"
                  accept={ACCEPT_ATTR}
                  onChange={handleFileSelect}
                  className="hidden"
                />
              </div>
              <p className="text-sm text-muted-foreground">
                GeoJSON: FeatureCollection with Point geometries. CSV: headers with lat/latitude and lon/longitude/lng.
              </p>
            </div>

            {/* Collection Name */}
            <div className="space-y-2">
              <Label htmlFor="name">Collection Name *</Label>
              <Input
                id="name"
                placeholder="e.g., Spain Tobacco Shops"
                value={collectionName}
                onChange={(e) => setCollectionName(e.target.value)}
                required
                disabled={loading}
              />
            </div>

            {/* Description */}
            <div className="space-y-2">
              <Label htmlFor="description">Description (Optional)</Label>
              <Input
                id="description"
                placeholder="Brief description of this POI collection"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                disabled={loading}
              />
            </div>

            {/* Submit Button */}
            <div className="flex justify-end space-x-4">
              <Link href="/pois">
                <Button type="button" variant="outline" disabled={loading}>
                  Cancel
                </Button>
              </Link>
              <Button type="submit" disabled={loading || !file || !preview?.isValid}>
                {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                Upload Collection
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>
    </MainLayout>
  )
}
