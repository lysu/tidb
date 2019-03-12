// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
// +build !debug

package plugin

import (
	"path/filepath"
	gplugin "plugin"
	"strconv"

	"github.com/pingcap/errors"
)

func loadOne(dir string, pluginID ID) (*Plugin, error) {
	var plugin Plugin
	plugin.Path = filepath.Join(dir, string(pluginID)+LibrarySuffix)
	library, err := gplugin.Open(plugin.Path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	manifestSym, err := library.Lookup(ManifestSymbol)
	if err != nil {
		return nil, errors.Trace(err)
	}
	manifest, ok := manifestSym.(func() *Manifest)
	if !ok {
		return nil, errInvalidPluginManifest.GenWithStackByArgs(string(pluginID))
	}
	pName, pVersion, err := pluginID.Decode()
	if err != nil {
		return nil, errors.Trace(err)
	}
	plugin.Manifest = manifest()
	if plugin.Name != pName {
		return nil, errInvalidPluginName.GenWithStackByArgs(string(pluginID), plugin.Name)
	}
	if strconv.Itoa(int(plugin.Version)) != pVersion {
		return nil, errInvalidPluginVersion.GenWithStackByArgs(string(pluginID))
	}
	return &plugin, nil
}
