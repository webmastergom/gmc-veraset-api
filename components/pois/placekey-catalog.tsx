'use client';

import { useState, useEffect } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Badge } from '@/components/ui/badge';
import { Loader2, CheckSquare, Square, Search } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';

export interface POIWithPlacekey {
  id: string;
  place_key?: string;
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

interface PlacekeyCatalogProps {
  onPOIsSelected: (pois: POIWithPlacekey[]) => void;
  selectedPOIs?: POIWithPlacekey[];
}

export default function PlacekeyCatalog({ onPOIsSelected, selectedPOIs = [] }: PlacekeyCatalogProps) {
  const { toast } = useToast();
  const [loading, setLoading] = useState(false);
  const [searching, setSearching] = useState(false);
  
  // Filter options
  const [topCategories, setTopCategories] = useState<string[]>([]);
  const [subCategories, setSubCategories] = useState<string[]>([]);
  const [brands, setBrands] = useState<string[]>([]);
  const [countries, setCountries] = useState<string[]>([]);
  const [states, setStates] = useState<string[]>([]);
  const [cities, setCities] = useState<string[]>([]);
  
  // Filters
  const [filters, setFilters] = useState({
    category: '',
    subcategory: '',
    brand: '',
    country: '',
    state: '',
    city: '',
  });
  
  // Search results
  const [searchResults, setSearchResults] = useState<POIWithPlacekey[]>([]);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(
    new Set(selectedPOIs.map(p => p.id))
  );
  const [totalResults, setTotalResults] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [limit] = useState(1000);
  const [offset, setOffset] = useState(0);

  // Load filter options on mount
  useEffect(() => {
    loadFilterOptions();
  }, []);

  const loadFilterOptions = async () => {
    setLoading(true);
    try {
      // Load top categories
      const topRes = await fetch('/api/pois/import/veraset?type=top', {
        credentials: 'include',
      });
      if (topRes.ok) {
        const topData = await topRes.json();
        setTopCategories(topData.categories || topData.data || []);
      }

      // Load countries
      const countriesRes = await fetch('/api/pois/import/veraset?type=countries', {
        credentials: 'include',
      });
      if (countriesRes.ok) {
        const countriesData = await countriesRes.json();
        setCountries(countriesData.countries || countriesData.data || []);
      }
    } catch (error) {
      console.error('Error loading filter options:', error);
    } finally {
      setLoading(false);
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
    setOffset(0);
    
    try {
      const requestBody: any = {
        limit,
        offset: 0,
      };

      if (filters.category) requestBody.category = filters.category;
      if (filters.subcategory) requestBody.subcategory = filters.subcategory;
      if (filters.brand) requestBody.brand = filters.brand;
      if (filters.country) requestBody.country = filters.country;
      if (filters.state) requestBody.state = filters.state;
      if (filters.city) requestBody.city = filters.city;

      const res = await fetch('/api/pois/import/veraset', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify(requestBody),
      });

      if (!res.ok) {
        const error = await res.json();
        throw new Error(error.error || 'Failed to search POIs');
      }

      const data = await res.json();
      const pois = data.pois || [];
      
      setSearchResults(pois);
      setTotalResults(data.total || pois.length);
      setHasMore(data.hasMore || false);
      
      if (pois.length === 0) {
        toast({
          title: 'No Results',
          description: 'No POIs found matching your filters',
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

  const togglePOI = (poi: POIWithPlacekey) => {
    const newSelected = new Set(selectedIds);
    if (newSelected.has(poi.id)) {
      newSelected.delete(poi.id);
    } else {
      newSelected.add(poi.id);
    }
    setSelectedIds(newSelected);

    // Update parent with selected POIs
    const allPOIs = [...searchResults, ...selectedPOIs];
    const selected = allPOIs.filter(p => newSelected.has(p.id));
    onPOIsSelected(selected);
  };

  const selectAll = () => {
    const allIds = new Set(searchResults.map(p => p.id));
    setSelectedIds(allIds);
    onPOIsSelected(searchResults);
  };

  const clearSelection = () => {
    setSelectedIds(new Set());
    onPOIsSelected([]);
  };

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle>Placekey Catalog</CardTitle>
          <CardDescription>
            Browse and select POIs from Veraset catalog by category, brand, or location. Selected POIs include placekeys for efficient movement queries.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="catalog-category">Category</Label>
              <Select
                value={filters.category || undefined}
                onValueChange={(value) => {
                  setFilters({ ...filters, category: value, subcategory: '' });
                  if (value) {
                    loadSubCategories(value);
                  } else {
                    setSubCategories([]);
                  }
                }}
              >
                <SelectTrigger id="catalog-category">
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

            {filters.category && (
              <div>
                <Label htmlFor="catalog-subcategory">Subcategory</Label>
                <Select
                  value={filters.subcategory || undefined}
                  onValueChange={(value) => {
                    setFilters({ ...filters, subcategory: value });
                    if (value) {
                      loadBrands();
                    }
                  }}
                >
                  <SelectTrigger id="catalog-subcategory">
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
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="catalog-brand">Brand</Label>
              <Input
                id="catalog-brand"
                placeholder="e.g., Starbucks"
                value={filters.brand}
                onChange={(e) => setFilters({ ...filters, brand: e.target.value })}
              />
            </div>

            <div>
              <Label htmlFor="catalog-country">Country</Label>
              <Select
                value={filters.country || undefined}
                onValueChange={(value) => {
                  setFilters({ ...filters, country: value, state: '', city: '' });
                  setCities([]);
                  if (value) {
                    loadStates(value);
                  } else {
                    loadStates();
                  }
                }}
              >
                <SelectTrigger id="catalog-country">
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
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="catalog-state">State</Label>
              <Select
                value={filters.state || undefined}
                onValueChange={(value) => {
                  setFilters({ ...filters, state: value, city: '' });
                  if (value) {
                    loadCities(value, filters.country);
                  } else {
                    setCities([]);
                  }
                }}
              >
                <SelectTrigger id="catalog-state">
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

            {filters.state && (
              <div>
                <Label htmlFor="catalog-city">City</Label>
                <Select
                  value={filters.city || undefined}
                  onValueChange={(value) => {
                    setFilters({ ...filters, city: value });
                    if (value) {
                      loadBrands();
                    }
                  }}
                >
                  <SelectTrigger id="catalog-city">
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
          </div>

          <Button
            onClick={handleSearch}
            disabled={searching || loading}
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
                Search Catalog
              </>
            )}
          </Button>
        </CardContent>
      </Card>

      {searchResults.length > 0 && (
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle>Catalog Results</CardTitle>
                <CardDescription>
                  {totalResults.toLocaleString()} POIs found. {selectedIds.size} selected.
                </CardDescription>
              </div>
              <div className="flex gap-2">
                <Button variant="outline" size="sm" onClick={selectAll}>
                  Select All
                </Button>
                <Button variant="outline" size="sm" onClick={clearSelection}>
                  Clear
                </Button>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="h-[400px] overflow-y-auto border rounded-md p-2 space-y-2">
              {searchResults.map((poi) => {
                const isSelected = selectedIds.has(poi.id);
                return (
                  <div
                    key={poi.id}
                    className={`p-3 border rounded-lg cursor-pointer hover:bg-secondary transition-colors ${
                      isSelected ? 'bg-secondary border-primary' : ''
                    }`}
                    onClick={() => togglePOI(poi)}
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
                        {poi.place_key && (
                          <div className="text-xs text-muted-foreground font-mono mt-1">
                            Placekey: {poi.place_key}
                          </div>
                        )}
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
          </CardContent>
        </Card>
      )}
    </div>
  );
}
