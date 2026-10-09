# 3D model credits

`teacher.glb` — "Michelle", a rigged character from Adobe Mixamo, as distributed in the three.js examples
(https://github.com/mrdoob/three.js/tree/dev/examples/models/gltf, file `Michelle.glb`). Unmodified; Fretwise poses its
skeleton at runtime (seated pose, inverse-kinematics fretting and strumming) and does not use its baked animations.

Mixamo characters may be used royalty-free in personal and commercial projects but may not be redistributed as
standalone assets. This private prototype ships it inside the app; before a public or commercial release, replace it with
a character the business owns or has a clear licence for (keep the standard Mixamo/Humanoid bone names — `*LeftHandIndex1`
etc. — so `teacher3d.js` works unchanged).

The guitar is not a downloaded model: `teacher3d.js` builds it in code to Yamaha F310 dimensions (634 mm scale, 43 mm nut,
dreadnought body) with procedurally generated wood textures, so it carries no third-party licence.
