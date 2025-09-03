param (
    [switch]$publish,
    [switch]$increment,
    [string]$hardcodeversion
)

function Increment-Version {
    param (
        [string]$versionString,
        [int]$maxPatch = 9,
        [int]$maxMinor = 9
    )

    # Remove leading 'v' if present
    $cleanVersion = $versionString.TrimStart('v')

    # Split into parts
    $parts = $cleanVersion.Split('.')

    if ($parts.Length -ne 3) {
        throw "Version string must be in format vX.Y.Z"
    }

    # Parse parts as integers
    $major = [int]$parts[0]
    $minor = [int]$parts[1]
    $patch = [int]$parts[2]

    # Increment patch and handle rollover
    $patch++
    if ($patch -gt $maxPatch) {
        $patch = 0
        $minor++
        if ($minor -gt $maxMinor) {
            $minor = 0
            $major++
        }
    }

    # Return new version string with 'v' prefix
    return "v$major.$minor.$patch"
}




$builds = @("x86_64-pc-windows-msvc", "x86_64-unknown-linux-gnu")


cargo install cross

$previous_tag = git tag --sort=-creatordate | head -n 1
$version = $previous_tag.Split('-')[0]

if ($increment){
    $version = Increment-Version $version
}

if ($hardcodeversion) {
    $version = $hardcodeversion
}

$current_time = Get-Date -Format "yyyy_MM_dd_HH_mm_ss"
$tag = $version + "-" + $current_time
git tag $tag

if ($publish){
    git push origin $tag
}


Write-Host "Building executables for $($builds.Count) architectures"

$finalized_executables = [System.Collections.ArrayList]@()
foreach ($item in $builds) {
    rustup target add $item
    cross build --target $item --release
    $outputDir = "target\$item\release"

    # Determine executable extension based on target OS
    if ($item -like "*windows*") {
        $exeName = "lava.exe"
        $newName = "lava-$version-$item.exe"
    }
    else {
        $exeName = "lava"
        $newName = "lava-$version-$item"
    }
    $target_path_full = "$outputDir\$newName"
    if (Test-Path $target_path_full) {
        Remove-Item $target_path_full
        Write-Host "Deleted old $target_path_full"
    }
    Rename-Item -Path "$outputDir\$exeName" -NewName $newName -ErrorAction SilentlyContinue
    Write-Host "Renamed $exeName to $newName"
    $finalized_executables.add($target_path_full)
}

Write-Output "Compiled $finalized_executables"

if ($publish){
    $authStatus = gh auth status 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Not authenticated. Run 'gh auth login'."
        exit
    }

    # Prepare the gh command arguments
    $args = @($tag) + $finalized_executables + @(
        '-t', "Lava $version"
    )

    # Execute the gh release create command with all args
    gh release create @args

}

