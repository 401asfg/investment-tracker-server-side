/**
 * A controller for vehicles
 */
@Controller
@ResponseBody
@RequestMapping("/vehicles")
class VehicleController {
    /**
     * TODO: write documentation
     */
    @GetMapping
    fun index(@RequestParam(name = "q") query: String): String {
        // TODO: implement stub
    }

    /**
     * @param id The id of the vehicle to get
     * @return The vehicle with the given id
     */
    @GetMapping
    fun show(@RequestParam id: Int): String {
        // TODO: implement stub
    }
}