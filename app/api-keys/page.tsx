'use client';

import { useState, useEffect, useCallback } from 'react';
import { MainLayout } from '@/components/layout/main-layout';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { useToast } from '@/hooks/use-toast';
import { Plus, Key, Copy, Trash2, Power, PowerOff, Loader2 } from 'lucide-react';

interface ApiKey {
  id: string;
  name: string;
  description?: string;
  active: boolean;
  createdAt: string;
  lastUsedAt?: string;
  usageCount: number;
}

export default function APIKeysPage() {
  const [keys, setKeys] = useState<ApiKey[]>([]);
  const [loading, setLoading] = useState(true);
  const [createDialogOpen, setCreateDialogOpen] = useState(false);
  const [newKeyName, setNewKeyName] = useState('');
  const [newKeyDescription, setNewKeyDescription] = useState('');
  const [creating, setCreating] = useState(false);
  const [newKeyPlain, setNewKeyPlain] = useState<string | null>(null);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [keyToDelete, setKeyToDelete] = useState<string | null>(null);
  const { toast } = useToast();

  const fetchKeys = useCallback(async () => {
    try {
      const res = await fetch('/api/api-keys');
      if (res.ok) {
        const data = await res.json();
        setKeys(data.keys || []);
      }
    } catch (error) {
      console.error('Error fetching API keys:', error);
      toast({
        title: 'Error',
        description: 'Failed to fetch API keys',
        variant: 'destructive',
      });
    } finally {
      setLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    fetchKeys();
  }, [fetchKeys]);

  const handleCreate = async () => {
    if (!newKeyName.trim()) {
      toast({
        title: 'Error',
        description: 'Name is required',
        variant: 'destructive',
      });
      return;
    }

    setCreating(true);
    try {
      const res = await fetch('/api/api-keys', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: newKeyName.trim(),
          description: newKeyDescription.trim() || undefined,
        }),
      });

      if (res.ok) {
        const data = await res.json();
        setNewKeyPlain(data.apiKey);
        setNewKeyName('');
        setNewKeyDescription('');
        setCreateDialogOpen(false);
        await fetchKeys();
      } else {
        const error = await res.json().catch(() => ({ error: 'Failed to create API key' }));
        toast({
          title: 'Error',
          description: error.error || error.message || 'Failed to create API key',
          variant: 'destructive',
        });
      }
    } catch (error) {
      console.error('Error creating API key:', error);
      toast({
        title: 'Error',
        description: 'Failed to create API key',
        variant: 'destructive',
      });
    } finally {
      setCreating(false);
    }
  };

  const handleToggleActive = async (id: string, currentActive: boolean) => {
    try {
      const res = await fetch(`/api/api-keys/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ active: !currentActive }),
      });

      if (res.ok) {
        await fetchKeys();
        toast({
          title: 'Success',
          description: `API key ${!currentActive ? 'activated' : 'revoked'}`,
        });
      } else {
        const error = await res.json().catch(() => ({ error: 'Failed to update API key' }));
        toast({
          title: 'Error',
          description: error.error || error.message || 'Failed to update API key',
          variant: 'destructive',
        });
      }
    } catch (error) {
      console.error('Error toggling API key:', error);
      toast({
        title: 'Error',
        description: 'Failed to update API key',
        variant: 'destructive',
      });
    }
  };

  const handleDelete = async () => {
    if (!keyToDelete) return;

    try {
      const res = await fetch(`/api/api-keys/${keyToDelete}`, {
        method: 'DELETE',
      });

      if (res.ok) {
        await fetchKeys();
        toast({
          title: 'Success',
          description: 'API key deleted',
        });
      } else {
        const error = await res.json().catch(() => ({ error: 'Failed to delete API key' }));
        toast({
          title: 'Error',
          description: error.error || error.message || 'Failed to delete API key',
          variant: 'destructive',
        });
      }
    } catch (error) {
      console.error('Error deleting API key:', error);
      toast({
        title: 'Error',
        description: 'Failed to delete API key',
        variant: 'destructive',
      });
    } finally {
      setDeleteDialogOpen(false);
      setKeyToDelete(null);
    }
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    toast({
      title: 'Copied',
      description: 'API key copied to clipboard',
    });
  };

  const formatDate = (dateString?: string) => {
    if (!dateString) return 'Never';
    return new Date(dateString).toLocaleString();
  };

  if (loading) {
    return (
      <MainLayout>
        <div className="flex items-center justify-center h-64">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      </MainLayout>
    );
  }

  return (
    <MainLayout>
      <div className="mb-8 flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white">API Keys</h1>
          <p className="text-gray-500 mt-2">
            Manage API keys for external clients
          </p>
        </div>
        <Button onClick={() => setCreateDialogOpen(true)}>
          <Plus className="mr-2 h-4 w-4" />
          Create API Key
        </Button>
      </div>

      {keys.length === 0 ? (
        <Card>
          <CardContent className="pt-6">
            <div className="text-center py-8">
              <Key className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
              <p className="text-muted-foreground">No API keys yet</p>
              <Button onClick={() => setCreateDialogOpen(true)} className="mt-4">
                Create your first API key
              </Button>
            </div>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-4">
          {keys.map((key) => (
            <Card key={key.id}>
              <CardHeader>
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <div className="flex items-center gap-2 mb-2">
                      <CardTitle>{key.name}</CardTitle>
                      <Badge variant={key.active ? 'default' : 'secondary'}>
                        {key.active ? 'Active' : 'Revoked'}
                      </Badge>
                    </div>
                    {key.description && (
                      <CardDescription>{key.description}</CardDescription>
                    )}
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                  <div>
                    <p className="text-sm text-muted-foreground">Created</p>
                    <p className="text-sm font-medium">{formatDate(key.createdAt)}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Last Used</p>
                    <p className="text-sm font-medium">{formatDate(key.lastUsedAt)}</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Usage Count</p>
                    <p className="text-sm font-medium">{key.usageCount.toLocaleString()}</p>
                  </div>
                </div>
                <div className="flex gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => handleToggleActive(key.id, key.active)}
                  >
                    {key.active ? (
                      <>
                        <PowerOff className="mr-2 h-4 w-4" />
                        Revoke
                      </>
                    ) : (
                      <>
                        <Power className="mr-2 h-4 w-4" />
                        Activate
                      </>
                    )}
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => {
                      setKeyToDelete(key.id);
                      setDeleteDialogOpen(true);
                    }}
                  >
                    <Trash2 className="mr-2 h-4 w-4" />
                    Delete
                  </Button>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Create Dialog */}
      <Dialog open={createDialogOpen} onOpenChange={setCreateDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create API Key</DialogTitle>
            <DialogDescription>
              Create a new API key for external clients. The key will be shown once.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div>
              <Label htmlFor="name">Name *</Label>
              <Input
                id="name"
                value={newKeyName}
                onChange={(e) => setNewKeyName(e.target.value)}
                placeholder="e.g., Client Name or Organization"
              />
            </div>
            <div>
              <Label htmlFor="description">Description</Label>
              <Input
                id="description"
                value={newKeyDescription}
                onChange={(e) => setNewKeyDescription(e.target.value)}
                placeholder="Optional description"
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setCreateDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={handleCreate} disabled={creating}>
              {creating ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Creating...
                </>
              ) : (
                'Create'
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* New Key Display Dialog */}
      <Dialog open={!!newKeyPlain} onOpenChange={() => setNewKeyPlain(null)}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>API Key Created</DialogTitle>
            <DialogDescription>
              Save this API key now. It will not be shown again.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div>
              <Label>API Key</Label>
              <div className="flex gap-2 mt-2">
                <Input
                  value={newKeyPlain || ''}
                  readOnly
                  className="font-mono text-sm"
                />
                <Button
                  variant="outline"
                  size="icon"
                  onClick={() => newKeyPlain && copyToClipboard(newKeyPlain)}
                >
                  <Copy className="h-4 w-4" />
                </Button>
              </div>
            </div>
            <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-4">
              <p className="text-sm text-yellow-800 dark:text-yellow-200">
                <strong>Warning:</strong> This is the only time you will see this API key.
                Make sure to copy it and store it securely.
              </p>
            </div>
          </div>
          <DialogFooter>
            <Button onClick={() => setNewKeyPlain(null)}>I&apos;ve Saved It</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete API Key</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete this API key? This action cannot be undone.
              Any clients using this key will no longer be able to access the API.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={handleDelete} className="bg-destructive text-destructive-foreground">
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </MainLayout>
  );
}
