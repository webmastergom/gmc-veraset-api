import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Label } from "@/components/ui/label"
import { Input } from "@/components/ui/input"

export const dynamic = 'force-dynamic';

async function getDatasets() {
  try {
    const { getAllJobs } = await import("@/lib/jobs");
    const jobs = await getAllJobs();
    
    const syncedJobs = jobs.filter((job) => job.s3DestPath);
    
    return syncedJobs.map((job) => {
      const datasetName = job.s3DestPath?.split("/").filter(Boolean).pop() || "unknown";
      return {
        name: datasetName,
        displayName: job.name,
      };
    });
  } catch (error) {
    console.error("Error fetching datasets:", error);
    return [];
  }
}

export default async function ExportPage() {
  const datasets = await getDatasets()

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold">Audience Export</h1>
        <p className="text-muted-foreground mt-2">
          Export device audiences for marketing activation
        </p>
      </div>

      <Card className="max-w-2xl">
        <CardHeader>
          <CardTitle>Export Configuration</CardTitle>
          <CardDescription>
            Configure filters and export format for your audience
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form className="space-y-6">
            <div className="space-y-2">
              <Label>Dataset *</Label>
              <Select>
                <SelectTrigger id="dataset" aria-label="Dataset">
                  <SelectValue placeholder="Select a dataset" />
                </SelectTrigger>
                <SelectContent>
                  {datasets.map((dataset) => (
                    <SelectItem key={dataset.name} value={dataset.name}>
                      {dataset.displayName}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label htmlFor="minDwell">Minimum Dwell Time (minutes)</Label>
              <Input
                id="minDwell"
                type="number"
                min="1"
                defaultValue="1"
                placeholder="1"
              />
              <p className="text-sm text-muted-foreground">
                Only include devices that spent at least this time at POIs
              </p>
            </div>

            <div className="space-y-2">
              <Label htmlFor="minPings">Minimum Pings per Device</Label>
              <Input
                id="minPings"
                type="number"
                min="1"
                defaultValue="1"
                placeholder="1"
              />
            </div>

            <div className="space-y-2">
              <Label>Export Format *</Label>
              <Select>
                <SelectTrigger id="format" aria-label="Export Format">
                  <SelectValue placeholder="Select format" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="csv">CSV (ad_id list)</SelectItem>
                  <SelectItem value="json">JSON (Full device profiles)</SelectItem>
                  <SelectItem value="liveramp">LiveRamp</SelectItem>
                  <SelectItem value="ttd">The Trade Desk</SelectItem>
                  <SelectItem value="dv360">DV360</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="rounded-md bg-muted p-4">
              <p className="text-sm font-medium mb-2">Export Preview</p>
              <div className="space-y-1 text-sm text-muted-foreground">
                <p>Qualifying devices: <span className="font-semibold text-foreground">~125,000</span></p>
                <p>Estimated match rate: <span className="font-semibold text-foreground">85%</span></p>
              </div>
            </div>

            <div className="flex justify-end">
              <Button type="submit">Generate Export</Button>
            </div>
          </form>
        </CardContent>
      </Card>
    </div>
  )
}
