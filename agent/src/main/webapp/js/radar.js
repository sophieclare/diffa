$(function() {

  Diffa.Views.Radar = Backbone.View.extend({

    period:   5,
    root2:    Math.sqrt(2) / 2,
    omega:    2 * Math.PI / 5,  // this.period
    diameter: 150,

    lineStyle : {
      strokeColor: 'green',
      strokeWidth: 1
    },

    diffs: [10,20,30],

    initialize: function() {
      _.bindAll(this, "render", "foo");
      this.render();
    },

    foo: function() {
      return this.diffs;
    },

    render: function() {
      $(this.el).html(JST['radar/radar']());

      paper.install(window);
      paper.setup('radar-canvas');

      this.centerRef = paper.view.center;

      this.center = new Point(this.centerRef.x, this.centerRef.y);
      this.outer = new Point(this.centerRef.x, this.centerRef.y + this.diameter);

      this.drawCrossHairs();
      this.drawCircles();


      var path = new Path();
      path.style = this.lineStyle;
      path.add(this.center);
      path.add(this.outer);

      var size = new Size(10, 10);
      var pos = new Point(this.centerRef.x - this.diameter/5, this.centerRef.y - this.diameter/5);
      blip = new Path.Rectangle(pos, size);
      blip.strokeColor = 'yellow';

      var pos2 = new Point(this.centerRef.x + this.diameter/5, this.centerRef.y + this.diameter/5);
      blip2 = new Path.Rectangle(pos2, size);
      blip2.strokeColor = 'blue';


      var self = this;

      view.onFrame = function onFrame(event) {

        var theta = event.time % self.period * self.omega;

        if (theta >= self.root2 && theta <= self.root2 + 0.1) {
          blip2.fillColor = 'blue';
        }
        else {
          blip2.fillColor = 'black';
        }

        var cx = Math.cos(theta) * self.diameter;
        var cy = Math.sin(theta) * self.diameter;

        path.segments[1].point.x = cx + self.centerRef.x;
        path.segments[1].point.y = cy + self.centerRef.y;

      }


      view.draw();

      return this;
    },

    drawCrossHairs: function() {
      var vertical = new Path();
      vertical.style = this.lineStyle;
      vertical.add(new Point(this.centerRef.x, this.centerRef.y - this.diameter));
      vertical.add(new Point(this.centerRef.x, this.centerRef.y + this.diameter));

      var horizontal = new Path();
      horizontal.style = this.lineStyle;
      horizontal.add(new Point(this.centerRef.x - this.diameter, this.centerRef.y ));
      horizontal.add(new Point(this.centerRef.x + this.diameter, this.centerRef.y ));
    },

    drawCircles: function() {
      var outerCircle = new Path.Circle(this.center, this.diameter);
      outerCircle.style = this.lineStyle;
      var middleCircle = new Path.Circle(this.center, this.diameter * 2/3);
      middleCircle.style = this.lineStyle;
      var innerCircle = new Path.Circle(this.center, this.diameter * 1/3);
      innerCircle.style = this.lineStyle;
    }

  });

  $('.diffa-radar').each(function() {
    var domain = Diffa.DomainManager.get($(this).data('domain'));
    new Diffa.Views.Radar({el: $(this), model: domain.blobs});
  });

});