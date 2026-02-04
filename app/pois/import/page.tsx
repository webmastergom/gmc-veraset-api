'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { MainLayout } from '@/components/layout/main-layout';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Badge } from '@/components/ui/badge';
import { Loader2, Search, MapPin, Download, CheckSquare, Square, ArrowLeft } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';

interface VerasetPOI {
  id: string;
  place_key?: string; // Critical: place_key is used for movement API instead of coordinates
  name: string;
  latitude: number;
  longitude: number;
  category?: string;
  subcategory?: string;
  brand?: string;
  address?: string;
  city?: string;
  state?: string;
  zipcode?: string;
  country?: string;
}

interface SearchFilters {
  category?: string;
  subcategory?: string;
  brand?: string;
  country?: string;
  state?: string;
  city?: string;
  zipcode?: string;
}

export default function POIImportPage() {
  const router = useRouter();
  const { toast } = useToast();
  
  const [loading, setLoading] = useState(false);
  const [searching, setSearching] = useState(false);
  const [loadingCategories, setLoadingCategories] = useState(false);
  
  // Filter options
  const [topCategories, setTopCategories] = useState<string[]>([]);
  const [subCategories, setSubCategories] = useState<string[]>([]);
  const [brands, setBrands] = useState<string[]>([]);
  const [countries, setCountries] = useState<string[]>([]);
  const [states, setStates] = useState<string[]>([]);
  const [cities, setCities] = useState<string[]>([]);
  
  // Search filters
  const [filters, setFilters] = useState<SearchFilters>({});
  
  // Search results
  const [searchResults, setSearchResults] = useState<VerasetPOI[]>([]);
  const [selectedPois, setSelectedPois] = useState<Set<string>>(new Set());
  const [totalResults, setTotalResults] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  
  // Collection name
  const [collectionName, setCollectionName] = useState('');
  const [collectionDescription, setCollectionDescription] = useState('');

  // Load filter options on mount
  useEffect(() => {
    loadFilterOptions();
  }, []);

  const loadFilterOptions = async () => {
    setLoadingCategories(true);
    try {
      // Load top categories
      const topRes = await fetch('/api/pois/import/veraset?type=top', {
        credentials: 'include',
      });
      if (topRes.ok) {
        const topData = await topRes.json();
        setTopCategories(topData.categories || topData.data || []);
      } else {
        const errorData = await topRes.json().catch(() => ({}));
        if (topRes.status === 401) {
          toast({
            title: 'Authentication Error',
            description: errorData.details || 'Veraset API key is not configured or invalid. Please check your VERASET_API_KEY environment variable.',
            variant: 'destructive',
          });
        }
      }

      // Load countries
      const countriesRes = await fetch('/api/pois/import/veraset?type=countries', {
        credentials: 'include',
      });
      if (countriesRes.ok) {
        const countriesData = await countriesRes.json();
        setCountries(countriesData.countries || countriesData.data || []);
      }

      // Load states (will be filtered by country if selected)
      if (!filters.country) {
        const statesRes = await fetch('/api/pois/import/veraset?type=states', {
          credentials: 'include',
        });
        if (statesRes.ok) {
          const statesData = await statesRes.json();
          setStates(statesData.states || statesData.data || []);
        }
      }
    } catch (error) {
      console.error('Error loading filter options:', error);
    } finally {
      setLoadingCategories(false);
    }
  };

  const loadSubCategories = async (category: string) => {
    if (!category) {
      setSubCategories([]);
      return;
    }
    
    try {
      const res = await fetch(`/api/pois/import/veraset?type=sub&category=${encodeURIComponent(category)}`, {
        credentials: 'include',
      });
      if (res.ok) {
        const data = await res.json();
        setSubCategories(data.subcategories || data.data || []);
      }
    } catch (error) {
      console.error('Error loading subcategories:', error);
    }
  };

  const loadBrands = async () => {
    const params = new URLSearchParams();
    if (filters.subcategory) params.append('subcategory', filters.subcategory);
    if (filters.state) params.append('state', filters.state);
    if (filters.city) params.append('city', filters.city);
    
    try {
      const res = await fetch(`/api/pois/import/veraset?type=brands&${params}`, {
        credentials: 'include',
      });
      if (res.ok) {
        const data = await res.json();
        setBrands(data.brands || data.data || []);
      }
    } catch (error) {
      console.error('Error loading brands:', error);
    }
  };

  const loadStates = async (country?: string) => {
    try {
      const url = country 
        ? `/api/pois/import/veraset?type=states&country=${encodeURIComponent(country)}`
        : '/api/pois/import/veraset?type=states';
      const res = await fetch(url, {
        credentials: 'include',
      });
      if (res.ok) {
        const data = await res.json();
        setStates(data.states || data.data || []);
      }
    } catch (error) {
      console.error('Error loading states:', error);
    }
  };

  const loadCities = async (state: string, country?: string) => {
    if (!state) {
      setCities([]);
      return;
    }
    
    try {
      const params = new URLSearchParams({ state });
      if (country) params.append('country', country);
      const res = await fetch(`/api/pois/import/veraset?type=cities&${params}`, {
        credentials: 'include',
      });
      if (res.ok) {
        const data = await res.json();
        setCities(data.cities || data.data || []);
      }
    } catch (error) {
      console.error('Error loading cities:', error);
    }
  };

  const handleSearch = async () => {
    setSearching(true);
    setSearchResults([]);
    setSelectedPois(new Set());
    
    try {
      // Clean filters - remove undefined values
      const cleanFilters: SearchFilters = {};
      if (filters.category) cleanFilters.category = filters.category;
      if (filters.subcategory) cleanFilters.subcategory = filters.subcategory;
      if (filters.brand) cleanFilters.brand = filters.brand;
      if (filters.country) cleanFilters.country = filters.country;
      if (filters.state) cleanFilters.state = filters.state;
      if (filters.city) cleanFilters.city = filters.city;
      if (filters.zipcode) cleanFilters.zipcode = filters.zipcode;

      const res = await fetch('/api/pois/import/veraset', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          ...cleanFilters,
          limit: 1000,
          offset: 0,
        }),
      });

      if (!res.ok) {
        const error = await res.json();
        throw new Error(error.error || 'Failed to search POIs');
      }

      const data = await res.json();
      setSearchResults(data.pois || []);
      setTotalResults(data.total || data.pois?.length || 0);
      setHasMore(data.hasMore || false);
      
      if (data.pois && data.pois.length === 0) {
        toast({
          title: 'No Results',
          description: 'No POIs found matching your criteria',
          variant: 'default',
        });
      }
    } catch (error: any) {
      toast({
        title: 'Search Error',
        description: error.message || 'Failed to search POIs',
        variant: 'destructive',
      });
    } finally {
      setSearching(false);
    }
  };

  const togglePOI = (poiId: string) => {
    const newSelected = new Set(selectedPois);
    if (newSelected.has(poiId)) {
      newSelected.delete(poiId);
    } else {
      newSelected.add(poiId);
    }
    setSelectedPois(newSelected);
  };

  const selectAll = () => {
    const allIds = new Set(searchResults.map(p => p.id));
    setSelectedPois(allIds);
  };

  const clearSelection = () => {
    setSelectedPois(new Set());
  };

  const convertToGeoJSON = (pois: VerasetPOI[]): any => {
    return {
      type: 'FeatureCollection',
      features: pois.map(poi => ({
        type: 'Feature',
        id: poi.id,
        geometry: {
          type: 'Point',
          coordinates: [poi.longitude, poi.latitude],
        },
        properties: {
          id: poi.id,
          place_key: poi.place_key, // Critical: Save place_key for use in movement API
          name: poi.name,
          category: poi.category,
          subcategory: poi.subcategory,
          brand: poi.brand,
          address: poi.address,
          city: poi.city,
          state: poi.state,
          zipcode: poi.zipcode,
          country: poi.country,
        },
      })),
    };
  };

  const handleImport = async () => {
    if (selectedPois.size === 0) {
      toast({
        title: 'No POIs Selected',
        description: 'Please select at least one POI to import',
        variant: 'destructive',
      });
      return;
    }

    if (!collectionName.trim()) {
      toast({
        title: 'Collection Name Required',
        description: 'Please provide a name for the collection',
        variant: 'destructive',
      });
      return;
    }

    setLoading(true);
    try {
      const selectedPOIList = searchResults.filter(p => selectedPois.has(p.id));
      const geojson = convertToGeoJSON(selectedPOIList);

      // Create collection via API
      const res = await fetch('/api/pois/collections', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          name: collectionName,
          description: collectionDescription || `Imported from Veraset API (${selectedPOIList.length} POIs)`,
          geojson,
          poiCount: selectedPOIList.length,
          sources: {
            veraset: selectedPOIList.length,
          },
        }),
      });

      if (!res.ok) {
        const error = await res.json();
        throw new Error(error.error || 'Failed to create collection');
      }

      const collection = await res.json();
      
      toast({
        title: 'Success',
        description: `Imported ${selectedPOIList.length} POIs into collection "${collectionName}"`,
      });

      router.push(`/pois/${collection.id}`);
    } catch (error: any) {
      toast({
        title: 'Import Error',
        description: error.message || 'Failed to import POIs',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  };

  return (
    <MainLayout>
      <div className="mb-6">
        <Button variant="ghost" className="mb-4" onClick={() => router.push('/pois')}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back to Collections
        </Button>
        <h1 className="text-3xl font-bold text-white">Import POIs from Veraset</h1>
        <p className="text-gray-500 mt-2">
          Search and import Points of Interest from Veraset&apos;s database
        </p>
      </div>

      <div className="grid gap-6 lg:grid-cols-3">
        {/* Search Filters */}
        <Card className="lg:col-span-1">
          <CardHeader>
            <CardTitle>Search Filters</CardTitle>
            <CardDescription>
              Filter POIs by category, brand, location, etc.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Category */}
            <div>
              <Label htmlFor="category">Category</Label>
              <Select
                value={filters.category || undefined}
                onValueChange={(value) => {
                  setFilters({ ...filters, category: value, subcategory: undefined });
                  if (value) {
                    loadSubCategories(value);
                  } else {
                    setSubCategories([]);
                  }
                }}
              >
                <SelectTrigger id="category">
                  <SelectValue placeholder="Select category" />
                </SelectTrigger>
                <SelectContent>
                  {topCategories.map((cat) => (
                    <SelectItem key={cat} value={cat}>
                      {cat}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Subcategory */}
            {filters.category && (
              <div>
                <Label htmlFor="subcategory">Subcategory</Label>
                <Select
                  value={filters.subcategory || undefined}
                  onValueChange={(value) => {
                    setFilters({ ...filters, subcategory: value });
                    if (value) {
                      loadBrands();
                    }
                  }}
                >
                  <SelectTrigger id="subcategory">
                    <SelectValue placeholder="Select subcategory" />
                  </SelectTrigger>
                  <SelectContent>
                    {subCategories.map((sub) => (
                      <SelectItem key={sub} value={sub}>
                        {sub}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}

            {/* Brand */}
            <div>
              <Label htmlFor="brand">Brand</Label>
              <Input
                id="brand"
                placeholder="e.g., Starbucks"
                value={filters.brand || ''}
                onChange={(e) => setFilters({ ...filters, brand: e.target.value })}
              />
            </div>

            {/* Country */}
            <div>
              <Label htmlFor="country">Country</Label>
              <Select
                value={filters.country || undefined}
                onValueChange={(value) => {
                  setFilters({ ...filters, country: value, state: undefined, city: undefined });
                  setCities([]);
                  if (value) {
                    loadStates(value);
                  } else {
                    loadStates();
                  }
                }}
              >
                <SelectTrigger id="country">
                  <SelectValue placeholder="Select country" />
                </SelectTrigger>
                <SelectContent>
                  {countries.map((country) => (
                    <SelectItem key={country} value={country}>
                      {country}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* State */}
            <div>
              <Label htmlFor="state">State</Label>
              <Select
                value={filters.state || undefined}
                onValueChange={(value) => {
                  setFilters({ ...filters, state: value, city: undefined });
                  if (value) {
                    loadCities(value, filters.country);
                  } else {
                    setCities([]);
                  }
                }}
              >
                <SelectTrigger id="state">
                  <SelectValue placeholder="Select state" />
                </SelectTrigger>
                <SelectContent>
                  {states.map((state) => (
                    <SelectItem key={state} value={state}>
                      {state}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* City */}
            {filters.state && (
              <div>
                <Label htmlFor="city">City</Label>
                <Select
                  value={filters.city || undefined}
                  onValueChange={(value) => {
                    setFilters({ ...filters, city: value });
                    if (value) {
                      loadBrands();
                    }
                  }}
                >
                  <SelectTrigger id="city">
                    <SelectValue placeholder="Select city" />
                  </SelectTrigger>
                  <SelectContent>
                    {cities.map((city) => (
                      <SelectItem key={city} value={city}>
                        {city}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}

            {/* Zipcode */}
            <div>
              <Label htmlFor="zipcode">Zipcode</Label>
              <Input
                id="zipcode"
                placeholder="e.g., 90210"
                value={filters.zipcode || ''}
                onChange={(e) => setFilters({ ...filters, zipcode: e.target.value })}
              />
            </div>

            <Button
              onClick={handleSearch}
              disabled={searching || loadingCategories}
              className="w-full"
            >
              {searching ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Searching...
                </>
              ) : (
                <>
                  <Search className="mr-2 h-4 w-4" />
                  Search POIs
                </>
              )}
            </Button>
          </CardContent>
        </Card>

        {/* Results */}
        <Card className="lg:col-span-2">
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle>Search Results</CardTitle>
                <CardDescription>
                  {searchResults.length > 0
                    ? `${totalResults.toLocaleString()} POIs found. ${selectedPois.size} selected.`
                    : 'Use filters to search for POIs'}
                </CardDescription>
              </div>
              {searchResults.length > 0 && (
                <div className="flex gap-2">
                  <Button variant="outline" size="sm" onClick={selectAll}>
                    Select All
                  </Button>
                  <Button variant="outline" size="sm" onClick={clearSelection}>
                    Clear
                  </Button>
                </div>
              )}
            </div>
          </CardHeader>
          <CardContent>
            {searchResults.length === 0 ? (
              <div className="text-center py-12 text-muted-foreground">
                <MapPin className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p>No results yet. Use the filters to search for POIs.</p>
              </div>
            ) : (
              <div className="h-[500px] overflow-y-auto border rounded-md p-2">
                <div className="space-y-2">
                  {searchResults.map((poi) => {
                    const isSelected = selectedPois.has(poi.id);
                    return (
                      <div
                        key={poi.id}
                        className={`p-3 border rounded-lg cursor-pointer hover:bg-secondary transition-colors ${
                          isSelected ? 'bg-secondary border-primary' : ''
                        }`}
                        onClick={() => togglePOI(poi.id)}
                      >
                        <div className="flex items-start gap-3">
                          <div className="mt-1">
                            {isSelected ? (
                              <CheckSquare className="h-5 w-5 text-primary" />
                            ) : (
                              <Square className="h-5 w-5 text-muted-foreground" />
                            )}
                          </div>
                          <div className="flex-1 min-w-0">
                            <div className="font-medium">{poi.name}</div>
                            <div className="text-sm text-muted-foreground mt-1">
                              {[poi.address, poi.city, poi.state, poi.zipcode]
                                .filter(Boolean)
                                .join(', ')}
                            </div>
                            <div className="flex gap-2 mt-2">
                              {poi.category && (
                                <Badge variant="secondary" className="text-xs">
                                  {poi.category}
                                </Badge>
                              )}
                              {poi.brand && (
                                <Badge variant="outline" className="text-xs">
                                  {poi.brand}
                                </Badge>
                              )}
                            </div>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Import Section */}
      {searchResults.length > 0 && (
        <Card className="mt-6">
          <CardHeader>
            <CardTitle>Import Selected POIs</CardTitle>
            <CardDescription>
              Create a new collection with the selected POIs
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid gap-4 md:grid-cols-2">
              <div>
                <Label htmlFor="collection-name">Collection Name *</Label>
                <Input
                  id="collection-name"
                  placeholder="e.g., Starbucks California"
                  value={collectionName}
                  onChange={(e) => setCollectionName(e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="collection-description">Description</Label>
                <Input
                  id="collection-description"
                  placeholder="Optional description"
                  value={collectionDescription}
                  onChange={(e) => setCollectionDescription(e.target.value)}
                />
              </div>
            </div>
            
            <div className="flex items-center justify-between pt-4 border-t">
              <div className="text-sm text-muted-foreground">
                {selectedPois.size} POI{selectedPois.size !== 1 ? 's' : ''} selected
              </div>
              <Button
                onClick={handleImport}
                disabled={loading || selectedPois.size === 0 || !collectionName.trim()}
              >
                {loading ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Importing...
                  </>
                ) : (
                  <>
                    <Download className="mr-2 h-4 w-4" />
                    Import {selectedPois.size} POI{selectedPois.size !== 1 ? 's' : ''}
                  </>
                )}
              </Button>
            </div>
          </CardContent>
        </Card>
      )}
    </MainLayout>
  );
}
