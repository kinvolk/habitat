import { NgModule } from "@angular/core";
import { Routes, RouterModule } from "@angular/router";
import { OriginPageComponent } from "./origin-page/origin-page.component";
import { OriginsPageComponent } from "./origins-page/origins-page.component";
import { OriginCreatePageComponent } from "./origin-create-page/origin-create-page.component";

const routes: Routes = [
  {
    path: "origins",
    component: OriginsPageComponent
  },
  {
    path: "origins/create",
    component: OriginCreatePageComponent
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class OriginRoutingModule {}
