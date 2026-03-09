/// Platform-aware constants that export the correct implementation
/// based on the compilation target.
library;

export 'constants_vm.dart' if (dart.library.js) 'constants_web.dart';
