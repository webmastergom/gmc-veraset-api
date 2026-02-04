"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import { X, Search, CheckSquare, Square } from "lucide-react"
// Using div with overflow instead of ScrollArea for simplicity

interface POI {
  poiId: string
  pings?: number
  devices?: number
  originalId?: string // Original ID from GeoJSON
  displayName?: string // Human-readable display name
}

interface POIFilterProps {
  availablePois: POI[]
  selectedPois: string[]
  onSelectionChange: (selectedPois: string[]) => void
  jobId?: string // Optional job ID to get POI mapping
}

export function POIFilter({ availablePois, selectedPois, onSelectionChange, jobId }: POIFilterProps) {
  const [searchQuery, setSearchQuery] = useState("")
  const [showAll, setShowAll] = useState(false)

  // Filter POIs based on search query
  const filteredPois = availablePois.filter(poi =>
    poi.poiId.toLowerCase().includes(searchQuery.toLowerCase())
  )

  // Show top 20 by default, or all if showAll is true
  const displayedPois = showAll ? filteredPois : filteredPois.slice(0, 20)

  const togglePOI = (poiId: string) => {
    if (selectedPois.includes(poiId)) {
      onSelectionChange(selectedPois.filter(id => id !== poiId))
    } else {
      onSelectionChange([...selectedPois, poiId])
    }
  }

  const selectAll = () => {
    onSelectionChange(displayedPois.map(poi => poi.poiId))
  }

  const clearAll = () => {
    onSelectionChange([])
  }

  const removePOI = (poiId: string) => {
    onSelectionChange(selectedPois.filter(id => id !== poiId))
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle>Filter by POIs</CardTitle>
            <CardDescription>
              Select one or more POIs to filter the analysis results
            </CardDescription>
          </div>
          {selectedPois.length > 0 && (
            <Badge variant="secondary">
              {selectedPois.length} selected
            </Badge>
          )}
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Search */}
        <div className="relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search POIs..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-9"
          />
        </div>

        {/* Selected POIs */}
        {selectedPois.length > 0 && (
          <div className="space-y-2">
            <Label className="text-sm font-medium">Selected POIs:</Label>
            <div className="flex flex-wrap gap-2">
              {selectedPois.map(poiId => {
                const poi = availablePois.find(p => p.poiId === poiId)
                return (
                  <Badge
                    key={poiId}
                    variant="secondary"
                    className="flex items-center gap-1 pr-1"
                  >
                    <span className="font-mono text-xs">{poiId}</span>
                    {poi?.devices && (
                      <span className="text-xs text-muted-foreground">
                        ({poi.devices} devices)
                      </span>
                    )}
                    <button
                      onClick={() => removePOI(poiId)}
                      className="ml-1 hover:bg-destructive/20 rounded-full p-0.5"
                    >
                      <X className="h-3 w-3" />
                    </button>
                  </Badge>
                )
              })}
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={clearAll}
              className="w-full"
            >
              Clear All
            </Button>
          </div>
        )}

        {/* POI List */}
        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <Label className="text-sm font-medium">
              Available POIs ({filteredPois.length})
            </Label>
            {displayedPois.length > 0 && (
              <Button
                variant="ghost"
                size="sm"
                onClick={selectAll}
                className="h-7 text-xs"
              >
                Select All Visible
              </Button>
            )}
          </div>
          <div className="h-[300px] border rounded-md p-2 overflow-y-auto">
            {displayedPois.length === 0 ? (
              <div className="text-center text-muted-foreground py-8">
                {searchQuery ? "No POIs found matching your search" : "No POIs available"}
              </div>
            ) : (
              <div className="space-y-1">
                {displayedPois.map(poi => {
                  const isSelected = selectedPois.includes(poi.poiId)
                  return (
                    <label
                      key={poi.poiId}
                      className="flex items-center space-x-2 p-2 rounded-md hover:bg-secondary cursor-pointer"
                      onClick={() => togglePOI(poi.poiId)}
                    >
                      <div className="flex-shrink-0">
                        {isSelected ? (
                          <CheckSquare className="h-4 w-4 text-primary" />
                        ) : (
                          <Square className="h-4 w-4 text-muted-foreground" />
                        )}
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="font-mono text-sm">
                          {poi.displayName || poi.originalId || poi.poiId}
                        </div>
                        {poi.poiId !== poi.originalId && poi.originalId && (
                          <div className="text-xs text-muted-foreground italic">
                            Veraset ID: {poi.poiId}
                          </div>
                        )}
                        {poi.devices !== undefined && poi.pings !== undefined && (
                          <div className="text-xs text-muted-foreground">
                            {poi.devices.toLocaleString()} devices • {poi.pings.toLocaleString()} pings
                          </div>
                        )}
                      </div>
                    </label>
                  )
                })}
              </div>
            )}
          </div>
          {filteredPois.length > 20 && !showAll && (
            <Button
              variant="outline"
              size="sm"
              onClick={() => setShowAll(true)}
              className="w-full"
            >
              Show All {filteredPois.length} POIs
            </Button>
          )}
          {showAll && filteredPois.length > 20 && (
            <Button
              variant="outline"
              size="sm"
              onClick={() => setShowAll(false)}
              className="w-full"
            >
              Show Less (Top 20)
            </Button>
          )}
        </div>
      </CardContent>
    </Card>
  )
}
